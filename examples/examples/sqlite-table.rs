use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    datasource::MemTable,
    error::DataFusionError,
    execution::{
        config::SessionConfig,
        context::{SessionContext, SessionState},
        runtime_env::RuntimeEnv,
    },
};
use datafusion_federation::{
    FederatedQueryPlanner, FederatedTableProviderAdaptor, FederationAnalyzerRule,
};
use datafusion_federation_sql::{connectorx::CXExecutor, SQLFederationProvider, SQLTableSource};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    init_tracing().map_err(DataFusionError::External)?;
    let dsn = "sqlite://./examples/examples/chinook.sqlite".to_string();
    // Register FederationAnalyzer
    // TODO: Interaction with other analyzers & optimizers.
    let state = SessionState::new_with_config_rt(
        SessionConfig::new().with_information_schema(true),
        Arc::new(RuntimeEnv::default()),
    )
    .add_analyzer_rule(Arc::new(FederationAnalyzerRule::new()))
    .with_query_planner(Arc::new(FederatedQueryPlanner::new()));
    let ctx = SessionContext::new_with_state(state);

    // Register schema
    // TODO: table inference
    let executor = Arc::new(CXExecutor::new(dsn)?);
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let table_provider_track = FederatedTableProviderAdaptor::new(Arc::new(
        SQLTableSource::new(Arc::clone(&provider), "Track".to_string()).await?,
    ));
    let table_provider_album = FederatedTableProviderAdaptor::new(Arc::new(
        SQLTableSource::new(Arc::clone(&provider), "Album".to_string()).await?,
    ));
    let table_provider_artist = FederatedTableProviderAdaptor::new(Arc::new(
        SQLTableSource::new(Arc::clone(&provider), "Artist".to_string()).await?,
    ));
    ctx.register_table("Track", Arc::new(table_provider_track))?;
    ctx.register_table("Album", Arc::new(table_provider_album))?;
    ctx.register_table("Artist", Arc::new(table_provider_artist))?;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trackid", DataType::Int64, false),
            Field::new("trackname", DataType::Utf8, false),
            Field::new("albumtitle", DataType::Utf8, false),
            Field::new("artistname", DataType::Utf8, false),
        ]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![11])),
                Arc::new(StringArray::from(vec!["Bittersweet Symphony"])),
                Arc::new(StringArray::from(vec!["Hooligans"])),
                Arc::new(StringArray::from(vec!["DF Jams"])),
            ],
        )
        .expect("record batch should not panic")
    }

    let extra_provider =
        MemTable::try_new(schema(), vec![vec![batch()]]).expect("mem table should not panic");

    ctx.register_table("Extra", Arc::new(extra_provider))?;

    // Run query
    let query = r#"EXPLAIN SELECT
             t.TrackId,
             t.Name AS TrackName,
             a.Title AS AlbumTitle,
             ar.Name AS ArtistName
         FROM Track t
         JOIN Album a ON t.AlbumId = a.AlbumId
         JOIN Artist ar ON a.ArtistId = ar.ArtistId
         UNION ALL
         select TrackId, TrackName, AlbumTitle, ArtistName from Extra
         LIMIT 10"#;
    let df = ctx.sql(query).await?;

    df.show().await
}

fn init_tracing() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter("debug")
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}
