//! Integration tests for event table SQL queries.
//!
//! These tests read real demo files and execute SQL queries against event tables,
//! validating the full pipeline from schema discovery through query execution.
//!
//! # Enabling debug logging
//!
//! Set the `RUST_LOG` environment variable to enable tracing output:
//! ```bash
//! RUST_LOG=demofusion=debug cargo test test_name -- --nocapture
//! ```

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use datafusion::arrow::array::Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use tokio::io::BufReader;

    use crate::datafusion::query_session::{StreamFormat, StreamingQuerySession};
    use crate::events::{EventType, event_schema};
    use crate::haste::async_demofile::AsyncDemoFile;
    use crate::haste::core::packet_source::PacketSource;
    use crate::haste::parser::AsyncStreamingParser;
    use crate::schema::EntitySchema;
    use crate::visitor::{DiscoveredSchemas, SchemaDiscoveryVisitor};

    fn init_tracing() {
        use tracing_subscriber::EnvFilter;
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }

    fn test_demo_path() -> String {
        std::env::var("TEST_DEMO_PATH").unwrap_or_else(|_| "test.dem".to_string())
    }

    fn demo_exists() -> bool {
        Path::new(&test_demo_path()).exists()
    }

    async fn discover_schemas_from_demo(path: &str) -> crate::error::Result<DiscoveredSchemas> {
        let file = tokio::fs::File::open(path).await.map_err(|e| {
            crate::error::Source2DfError::Haste(format!("Failed to open demo: {}", e))
        })?;
        let reader = BufReader::new(file);
        let demo_file = AsyncDemoFile::start_reading(reader).await.map_err(|e| {
            crate::error::Source2DfError::Haste(format!("Failed to read demo: {}", e))
        })?;

        let visitor = SchemaDiscoveryVisitor::new();
        let symbols_handle = visitor.symbols_handle();

        let mut parser = AsyncStreamingParser::from_stream_with_visitor(demo_file, visitor)
            .map_err(|e| crate::error::Source2DfError::Haste(e.to_string()))?;

        let _ = parser.run_to_end().await;

        let ctx = parser.context();
        let symbols = symbols_handle
            .lock()
            .map_err(|_| {
                crate::error::Source2DfError::Schema("Failed to lock symbols".to_string())
            })?
            .take()
            .ok_or_else(|| {
                crate::error::Source2DfError::Schema("No symbols discovered".to_string())
            })?;

        DiscoveredSchemas::from_context(ctx, symbols)
    }

    // =========================================================================
    // Schema Discovery Tests
    // =========================================================================

    #[test]
    fn test_all_event_types_have_schemas() {
        for event_type in EventType::all() {
            let schema = event_schema(event_type.table_name());
            assert!(
                schema.is_some(),
                "EventType::{:?} (table_name={}) should have a schema",
                event_type,
                event_type.table_name()
            );

            let schema = schema.unwrap();
            assert!(
                schema.field_with_name("tick").is_ok(),
                "Event schema for {:?} should have a tick column",
                event_type
            );
        }
    }

    #[test]
    fn test_event_schema_field_counts() {
        let damage_schema = event_schema("DamageEvent").expect("DamageEvent schema");
        assert!(
            damage_schema.fields().len() > 5,
            "DamageEvent should have multiple fields, got {}",
            damage_schema.fields().len()
        );

        let hero_killed_schema = event_schema("HeroKilledEvent").expect("HeroKilledEvent schema");
        assert!(
            hero_killed_schema.fields().len() > 3,
            "HeroKilledEvent should have multiple fields, got {}",
            hero_killed_schema.fields().len()
        );
    }

    #[test]
    fn test_damage_event_schema_fields() {
        let schema = event_schema("DamageEvent").expect("DamageEvent schema");

        let expected_fields = [
            "tick",
            "damage",
            "entindex_victim",
            "entindex_attacker",
            "victim_health_new",
        ];

        for field_name in expected_fields {
            assert!(
                schema.field_with_name(field_name).is_ok(),
                "DamageEvent should have field '{}'",
                field_name
            );
        }
    }

    #[test]
    fn test_event_type_table_name_mapping() {
        assert_eq!(EventType::Damage.table_name(), "DamageEvent");
        assert_eq!(EventType::HeroKilled.table_name(), "HeroKilledEvent");
        assert_eq!(EventType::BulletHit.table_name(), "BulletHitEvent");
        assert_eq!(
            EventType::ModifierApplied.table_name(),
            "ModifierAppliedEvent"
        );
    }

    #[test]
    fn test_event_type_message_ids_are_unique() {
        let mut seen_ids = std::collections::HashSet::new();
        for event_type in EventType::all() {
            let msg_id = event_type.message_id();
            assert!(
                seen_ids.insert(msg_id),
                "Duplicate message_id {} for {:?}",
                msg_id,
                event_type
            );
        }
    }

    #[test]
    fn test_event_type_all_returns_all_variants() {
        let all_events = EventType::all();
        assert!(
            all_events.len() >= 50,
            "Should have at least 50 event types, got {}",
            all_events.len()
        );
    }

    // =========================================================================
    // Demo File Schema Discovery Tests
    // =========================================================================

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_discover_entity_schemas_from_demo() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found at {}", test_demo_path());
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_discover_entity_schemas_from_demo_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_discover_entity_schemas_from_demo_inner() {
        let discovered = discover_schemas_from_demo(&test_demo_path()).await.unwrap();

        let entity_names = discovered.serializer_names();
        assert!(
            entity_names.len() > 50,
            "Should discover many entity types, got {}",
            entity_names.len()
        );

        assert!(
            entity_names.iter().any(|n| *n == "CCitadelPlayerPawn"),
            "Should discover CCitadelPlayerPawn"
        );
        assert!(
            entity_names
                .iter()
                .any(|n| *n == "CCitadelPlayerController"),
            "Should discover CCitadelPlayerController"
        );
    }

    // =========================================================================
    // Query Execution Tests (using in-memory demo buffer)
    // =========================================================================

    struct DemoFilePacketSource {
        data: Vec<u8>,
        position: usize,
        chunk_size: usize,
    }

    impl DemoFilePacketSource {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data,
                position: 0,
                chunk_size: 65536,
            }
        }
    }

    impl PacketSource for DemoFilePacketSource {
        async fn recv(&mut self) -> Option<bytes::Bytes> {
            if self.position >= self.data.len() {
                return None;
            }
            let end = std::cmp::min(self.position + self.chunk_size, self.data.len());
            let chunk = bytes::Bytes::copy_from_slice(&self.data[self.position..end]);
            self.position = end;
            Some(chunk)
        }
    }

    async fn load_demo_bytes() -> Option<Vec<u8>> {
        if !demo_exists() {
            return None;
        }
        tokio::fs::read(test_demo_path()).await.ok()
    }

    async fn get_entity_schemas() -> Option<HashMap<Arc<str>, EntitySchema>> {
        if !demo_exists() {
            return None;
        }
        let discovered = discover_schemas_from_demo(&test_demo_path()).await.ok()?;
        Some(
            discovered
                .schemas
                .into_iter()
                .map(|(_, schema)| (Arc::clone(&schema.serializer_name), schema))
                .collect(),
        )
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_query_damage_event_schema_only() {
        // Test that we can plan a query against DamageEvent without actually executing
        // This validates schema registration works
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let schema = event_schema("DamageEvent").expect("DamageEvent schema");

        // Verify the schema is valid for DataFusion
        assert!(schema.field_with_name("tick").is_ok());
        assert!(schema.field_with_name("damage").is_ok());
        assert!(schema.field_with_name("entindex_victim").is_ok());

        eprintln!("DamageEvent schema fields:");
        for field in schema.fields() {
            eprintln!("  {}: {:?}", field.name(), field.data_type());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_query_event_table_e2e() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_query_event_table_e2e_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_query_event_table_e2e_inner() {
        eprintln!("[test] Loading demo bytes...");
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Loaded {} bytes", demo_bytes.len());

        eprintln!("[test] Discovering schemas...");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        eprintln!("[test] Discovered {} entity schemas", entity_schemas.len());

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, damage, entindex_attacker, entindex_victim FROM DamageEvent WHERE damage > 0 LIMIT 100".to_string(),
        ];

        eprintln!("[test] Starting run_queries...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(32),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 1);

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut batch_count = 0;

        eprintln!("[test] Starting to consume stream...");

        while let Some(result) = stream.next().await {
            batch_count += 1;
            let batch = result.expect("batch");
            let batch_rows = batch.num_rows();
            total_rows += batch_rows;
            eprintln!(
                "[test] Batch {}: received {} rows (total: {})",
                batch_count, batch_rows, total_rows
            );
            batches.push(batch);
        }

        eprintln!(
            "[test] Stream consumption complete: {} batches, {} total rows",
            batches.len(),
            total_rows
        );

        assert!(
            total_rows > 0,
            "Should receive DamageEvent events from demo"
        );

        if let Some(batch) = batches.first() {
            assert!(batch.schema().field_with_name("tick").is_ok());
            assert!(batch.schema().field_with_name("damage").is_ok());
        }
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_query_multiple_event_types() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_query_multiple_event_types_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_query_multiple_event_types_inner() {
        eprintln!("[test] Loading demo bytes...");
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Loaded {} bytes", demo_bytes.len());

        eprintln!("[test] Discovering schemas...");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        eprintln!("[test] Discovered {} entity schemas", entity_schemas.len());

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, objective_team, objective_id FROM BossDamagedEvent LIMIT 50".to_string(),
            "SELECT tick, killing_team, player_pawn FROM RejuvStatusEvent LIMIT 50".to_string(),
        ];

        eprintln!("[test] Starting run_queries...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 2, "Should have 2 query streams");

        // IMPORTANT: Consume streams concurrently to avoid deadlock
        let stream_futures: Vec<_> = streams
            .into_iter()
            .enumerate()
            .map(|(i, mut stream)| async move {
                let mut rows = 0;
                let mut batch_count = 0;
                eprintln!("[test] Stream {} starting to consume", i);
                while let Some(result) = stream.next().await {
                    let batch = result.expect("batch");
                    batch_count += 1;
                    rows += batch.num_rows();
                    eprintln!(
                        "[test] Stream {} batch {}: {} rows (total: {})",
                        i,
                        batch_count,
                        batch.num_rows(),
                        rows
                    );
                }
                eprintln!("[test] Stream {} completed with {} rows", i, rows);
                rows
            })
            .collect();

        eprintln!("[test] Consuming streams concurrently...");
        // Use futures::future::join_all to consume all streams concurrently
        let results = futures::future::join_all(stream_futures).await;
        for (i, rows) in results.iter().enumerate() {
            eprintln!("[test] Query {} final: {} rows", i, rows);
        }
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_query_entity_and_event_together() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_query_entity_and_event_together_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_query_entity_and_event_together_inner() {
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 50".to_string(),
            "SELECT tick, objective_team, entity_damaged FROM BossDamagedEvent LIMIT 50"
                .to_string(),
        ];

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        assert_eq!(streams.len(), 2);

        let mut streams_vec: Vec<_> = streams.into_iter().collect();
        let entity_stream = streams_vec.remove(0);
        let event_stream = streams_vec.remove(0);

        let (entity_rows, event_rows) = tokio::join!(
            async {
                let mut rows = 0;
                let mut stream = entity_stream;
                while let Some(result) = stream.next().await {
                    rows += result.expect("batch").num_rows();
                }
                rows
            },
            async {
                let mut rows = 0;
                let mut stream = event_stream;
                while let Some(result) = stream.next().await {
                    rows += result.expect("batch").num_rows();
                }
                rows
            }
        );

        eprintln!("Entity rows: {}, Event rows: {}", entity_rows, event_rows);

        assert!(entity_rows > 0, "Should have entity data");
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_query_damage_event_with_nested_messages() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_query_damage_event_with_nested_messages_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_query_damage_event_with_nested_messages_inner() {
        eprintln!("[test] Loading demo bytes...");
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Loaded {} bytes", demo_bytes.len());

        eprintln!("[test] Discovering schemas...");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        eprintln!("[test] Discovered {} entity schemas", entity_schemas.len());

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // Query DamageEvent which has nested CMsgVector fields (origin.x, origin.y, origin.z)
        let queries = vec![
            "SELECT tick, damage, entindex_victim, entindex_attacker, origin FROM DamageEvent LIMIT 100".to_string(),
        ];

        eprintln!("[test] Starting run_queries...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 1);

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut batch_count = 0;

        eprintln!("[test] Starting to consume stream...");

        while let Some(result) = stream.next().await {
            batch_count += 1;
            let batch = result.expect("batch");
            let batch_rows = batch.num_rows();
            total_rows += batch_rows;
            eprintln!(
                "[test] Batch {}: received {} rows (total: {})",
                batch_count, batch_rows, total_rows
            );
            batches.push(batch);
        }

        eprintln!(
            "[test] Stream consumption complete: {} batches, {} total rows",
            batches.len(),
            total_rows
        );
        eprintln!("[test] Dropping stream...");
        drop(stream);
        eprintln!("[test] Stream dropped");

        assert!(
            total_rows > 0,
            "Should receive DamageEvent events from demo"
        );

        // Verify schema includes nested origin field
        if let Some(batch) = batches.first() {
            let schema = batch.schema();
            assert!(schema.field_with_name("tick").is_ok());
            assert!(schema.field_with_name("damage").is_ok());
            assert!(
                schema.field_with_name("origin").is_ok(),
                "Should have origin (nested CMsgVector) field"
            );

            // Log the origin field type to verify it's a struct
            let origin_field = schema.field_with_name("origin").unwrap();
            eprintln!("[test] origin field type: {:?}", origin_field.data_type());
        }

        eprintln!("[test] Assertions passed, test function ending");
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_query_hero_killed_event_with_list_field() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_query_hero_killed_event_with_list_field_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_query_hero_killed_event_with_list_field_inner() {
        eprintln!("[test] Loading demo bytes...");
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Loaded {} bytes", demo_bytes.len());

        eprintln!("[test] Discovering schemas...");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        eprintln!("[test] Discovered {} entity schemas", entity_schemas.len());

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // Query HeroKilledEvent which has repeated int32 entindex_assisters
        let queries = vec![
            "SELECT tick, entindex_victim, entindex_attacker, entindex_assisters FROM HeroKilledEvent LIMIT 50".to_string(),
        ];

        eprintln!("[test] Starting run_queries...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 1);

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut batch_count = 0;

        eprintln!("[test] Starting to consume stream...");

        while let Some(result) = stream.next().await {
            batch_count += 1;
            let batch = result.expect("batch");
            let batch_rows = batch.num_rows();
            total_rows += batch_rows;
            eprintln!(
                "[test] Batch {}: received {} rows (total: {})",
                batch_count, batch_rows, total_rows
            );
            batches.push(batch);
        }

        eprintln!(
            "[test] Stream consumption complete: {} batches, {} total rows",
            batches.len(),
            total_rows
        );
        eprintln!("[test] Dropping stream...");
        drop(stream);
        eprintln!("[test] Stream dropped");

        // Hero kills may be rare in a demo, so we don't assert > 0
        // But we verify the schema is correct
        let schema = event_schema("HeroKilledEvent").expect("HeroKilledEvent schema");
        assert!(schema.field_with_name("tick").is_ok());
        assert!(schema.field_with_name("entindex_victim").is_ok());
        assert!(
            schema.field_with_name("entindex_assisters").is_ok(),
            "Should have entindex_assisters (list) field"
        );

        // Log the assisters field type to verify it's a list
        let assisters_field = schema.field_with_name("entindex_assisters").unwrap();
        eprintln!(
            "[test] entindex_assisters field type: {:?}",
            assisters_field.data_type()
        );

        eprintln!("[test] Assertions passed, test function ending");
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_query_recent_damage_summary_event_with_list_of_messages() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_query_recent_damage_summary_event_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_query_recent_damage_summary_event_inner() {
        eprintln!("[test] Loading demo bytes...");
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Loaded {} bytes", demo_bytes.len());

        eprintln!("[test] Discovering schemas...");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        eprintln!("[test] Discovered {} entity schemas", entity_schemas.len());

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, player_slot, damage_records, modifier_records, total_damage FROM RecentDamageSummaryEvent LIMIT 50".to_string(),
        ];

        eprintln!("[test] Starting run_queries...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 1);

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut batch_count = 0;

        eprintln!("[test] Starting to consume stream...");

        while let Some(result) = stream.next().await {
            batch_count += 1;
            let batch = result.expect("batch");
            let batch_rows = batch.num_rows();
            total_rows += batch_rows;
            eprintln!(
                "[test] Batch {}: received {} rows (total: {})",
                batch_count, batch_rows, total_rows
            );
            batches.push(batch);
        }

        eprintln!(
            "[test] Stream consumption complete: {} batches, {} total rows",
            batches.len(),
            total_rows
        );
        eprintln!("[test] Dropping stream...");
        drop(stream);
        eprintln!("[test] Stream dropped");

        // RecentDamageSummary events should exist in a normal demo
        // Verify the schema is correct for list of messages
        let schema =
            event_schema("RecentDamageSummaryEvent").expect("RecentDamageSummaryEvent schema");
        assert!(schema.field_with_name("tick").is_ok());
        assert!(schema.field_with_name("player_slot").is_ok());
        assert!(
            schema.field_with_name("damage_records").is_ok(),
            "Should have damage_records (list of struct) field"
        );
        assert!(
            schema.field_with_name("modifier_records").is_ok(),
            "Should have modifier_records (list of struct) field"
        );

        // Log the damage_records field type to verify it's a list of structs
        let damage_records_field = schema.field_with_name("damage_records").unwrap();
        eprintln!(
            "[test] damage_records field type: {:?}",
            damage_records_field.data_type()
        );

        // Verify the inner struct has expected fields
        if let datafusion::arrow::datatypes::DataType::List(inner_field) =
            damage_records_field.data_type()
        {
            eprintln!("[test] damage_records inner field: {:?}", inner_field);
            if let datafusion::arrow::datatypes::DataType::Struct(fields) = inner_field.data_type()
            {
                let field_names: Vec<_> = fields.iter().map(|f| f.name().as_str()).collect();
                eprintln!("[test] DamageRecord struct fields: {:?}", field_names);
                assert!(
                    field_names.contains(&"damage"),
                    "DamageRecord should have 'damage' field"
                );
                assert!(
                    field_names.contains(&"hits"),
                    "DamageRecord should have 'hits' field"
                );
                assert!(
                    field_names.contains(&"hero_id"),
                    "DamageRecord should have 'hero_id' field"
                );
            }
        }

        eprintln!("[test] Assertions passed, test function ending");
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_aggregation_rejected_on_unbounded_stream() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec!["SELECT COUNT(*) as total FROM DamageEvent".to_string()];

        let result = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await;

        match result {
            Err(e) => {
                let err = e.to_string();
                assert!(
                    err.contains("pipeline breaking") || err.contains("SanityCheckPlan"),
                    "Error should mention pipeline breaking, got: {}",
                    err
                );
                eprintln!("[test] Correctly rejected aggregation: {}", err);
            }
            Ok(_) => {
                panic!("Aggregation on unbounded stream should be rejected");
            }
        }
    }

    // =========================================================================
    // JOIN Tests
    // =========================================================================
    //
    // NOTE: JOINs between event tables use SymmetricHashJoin for streaming.
    // Both sides are unbounded with tick ordering, enabling streaming joins.
    // Session must use target_partitions=1 to avoid RepartitionExec deadlocks.

    #[tokio::test]
    async fn test_join_event_tables_plan_selection() {
        // Verify that JOINing two event tables selects SymmetricHashJoinExec
        // with correct streaming configuration (no RepartitionExec)
        use crate::datafusion::table_providers::{EventTableProvider, new_receiver_slot};
        use crate::events::{EventType, event_schema};
        use datafusion::prelude::*;

        // Use streaming-compatible session config (target_partitions=1)
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);
        let ctx = SessionContext::new_with_config(config);

        // Register two event tables with mock receiver slots
        let damage_schema = event_schema("DamageEvent").expect("damage schema");
        let damage_slot = new_receiver_slot();
        let damage_provider =
            EventTableProvider::new(EventType::Damage, damage_schema, damage_slot);

        let kill_schema = event_schema("HeroKilledEvent").expect("kill schema");
        let kill_slot = new_receiver_slot();
        let kill_provider = EventTableProvider::new(EventType::HeroKilled, kill_schema, kill_slot);

        ctx.register_table("DamageEvent", std::sync::Arc::new(damage_provider))
            .unwrap();
        ctx.register_table("HeroKilledEvent", std::sync::Arc::new(kill_provider))
            .unwrap();

        // Plan a JOIN query
        let sql = "SELECT d.tick, d.damage, k.entindex_victim \
                   FROM DamageEvent d \
                   INNER JOIN HeroKilledEvent k ON d.tick = k.tick";

        let logical = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("logical plan");
        let physical = ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .expect("physical plan");

        let plan_str = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();

        eprintln!("[test] Physical plan:\n{}", plan_str);

        // Should use SymmetricHashJoinExec for streaming unbounded sources
        assert!(
            plan_str.contains("SymmetricHashJoinExec"),
            "Expected SymmetricHashJoinExec for event table JOIN, got:\n{}",
            plan_str
        );

        // Should NOT have RepartitionExec which can deadlock on streaming sources
        assert!(
            !plan_str.contains("RepartitionExec"),
            "Should not have RepartitionExec with target_partitions=1, got:\n{}",
            plan_str
        );
    }

    #[tokio::test]
    async fn test_join_pipeline_properties() {
        use crate::datafusion::pipeline_analysis::analyze_pipeline;
        use crate::datafusion::table_providers::{EventTableProvider, new_receiver_slot};
        use crate::events::{EventType, event_schema};
        use datafusion::prelude::*;

        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);
        let ctx = SessionContext::new_with_config(config);

        let damage_schema = event_schema("DamageEvent").expect("damage schema");
        let damage_slot = new_receiver_slot();
        let damage_provider =
            EventTableProvider::new(EventType::Damage, damage_schema, damage_slot);

        let kill_schema = event_schema("HeroKilledEvent").expect("kill schema");
        let kill_slot = new_receiver_slot();
        let kill_provider = EventTableProvider::new(EventType::HeroKilled, kill_schema, kill_slot);

        ctx.register_table("DamageEvent", std::sync::Arc::new(damage_provider))
            .unwrap();
        ctx.register_table("HeroKilledEvent", std::sync::Arc::new(kill_provider))
            .unwrap();

        let sql = "SELECT d.tick, d.damage, k.entindex_victim \
                   FROM DamageEvent d \
                   INNER JOIN HeroKilledEvent k ON d.tick = k.tick";

        let logical = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("logical plan");
        let physical = ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .expect("physical plan");

        let analysis = analyze_pipeline(&physical);
        eprintln!("\n{}\n", analysis.report());

        assert!(
            analysis.is_streaming_safe(),
            "Physical plan has pipeline breakers! This will cause streaming delay.\n{}",
            analysis.report()
        );
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_join_damage_with_hero_killed() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_join_damage_with_hero_killed_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_join_damage_with_hero_killed_inner() {
        use std::time::Instant;
        let start = Instant::now();

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Demo loaded at {:?}", start.elapsed());

        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // JOIN damage events with hero kill events on tick
        // Find damage events that happened on the same tick as a hero kill
        let queries = vec![
            "SELECT \
                d.tick, \
                d.damage, \
                d.entindex_attacker, \
                d.entindex_victim as damage_victim, \
                k.entindex_victim as killed_victim \
             FROM DamageEvent d \
             INNER JOIN HeroKilledEvent k ON d.tick = k.tick \
             WHERE d.damage > 0"
                .to_string(),
        ];

        eprintln!("[test] Running JOIN query at {:?}...", start.elapsed());
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();
        eprintln!("[test] Streams created at {:?}", start.elapsed());

        assert_eq!(streams.len(), 1, "Should have 1 stream");

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut first_batch = true;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            total_rows += batch.num_rows();
            if first_batch {
                eprintln!(
                    "[test] FIRST batch at {:?}: {} rows",
                    start.elapsed(),
                    batch.num_rows()
                );
                first_batch = false;
            } else {
                eprintln!(
                    "[test] batch: {} rows (total: {}) at {:?}",
                    batch.num_rows(),
                    total_rows,
                    start.elapsed()
                );
            }
        }

        eprintln!(
            "[test] Total JOIN results: {} at {:?}",
            total_rows,
            start.elapsed()
        );
        // JOIN may produce 0 rows if no damage happened on exact kill ticks, that's OK
    }

    #[tokio::test]
    async fn test_join_event_entity_plan_selection() {
        // Verify physical plan for event-entity JOIN
        use crate::datafusion::table_providers::{
            EntityTableProvider, EventTableProvider, new_receiver_slot,
        };
        use crate::events::{EventType, event_schema};
        use datafusion::prelude::*;

        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);
        let ctx = SessionContext::new_with_config(config);

        // Create event table provider
        let damage_schema = event_schema("DamageEvent").expect("damage schema");
        let damage_slot = new_receiver_slot();
        let damage_provider =
            EventTableProvider::new(EventType::Damage, damage_schema, damage_slot);
        ctx.register_table("DamageEvent", std::sync::Arc::new(damage_provider))
            .unwrap();

        // Create entity table provider using EntityTableProvider
        let entity_schema = datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "tick",
                datafusion::arrow::datatypes::DataType::Int32,
                false,
            ),
            datafusion::arrow::datatypes::Field::new(
                "entity_index",
                datafusion::arrow::datatypes::DataType::Int32,
                false,
            ),
            datafusion::arrow::datatypes::Field::new(
                "delta_type",
                datafusion::arrow::datatypes::DataType::Utf8,
                false,
            ),
        ]);
        let entity_slot = new_receiver_slot();
        let entity_provider = EntityTableProvider::new(
            std::sync::Arc::new(entity_schema),
            std::sync::Arc::from("CCitadelPlayerPawn"),
            entity_slot,
        );
        ctx.register_table("CCitadelPlayerPawn", std::sync::Arc::new(entity_provider))
            .unwrap();

        let sql = "SELECT d.tick, d.damage, p.entity_index \
                   FROM DamageEvent d \
                   INNER JOIN CCitadelPlayerPawn p ON d.tick = p.tick";

        let logical = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .expect("logical plan");
        let physical = ctx
            .state()
            .create_physical_plan(&logical)
            .await
            .expect("physical plan");

        let plan_str = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();

        eprintln!("[test] Event-Entity JOIN Physical plan:\n{}", plan_str);

        // Should use SymmetricHashJoinExec for streaming unbounded sources
        assert!(
            plan_str.contains("SymmetricHashJoinExec"),
            "Expected SymmetricHashJoinExec for event-entity table JOIN, got:\n{}",
            plan_str
        );

        // Should NOT have RepartitionExec
        assert!(
            !plan_str.contains("RepartitionExec"),
            "Should not have RepartitionExec with target_partitions=1, got:\n{}",
            plan_str
        );
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_join_event_with_entity_table() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_join_event_with_entity_table_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_join_event_with_entity_table_inner() {
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // Simple JOIN: just tick equality, no secondary condition
        // This helps isolate whether the issue is the JOIN itself or the compound condition
        let queries = vec![
            "SELECT \
                d.tick, \
                d.damage, \
                p.entity_index as pawn_idx \
             FROM DamageEvent d \
             INNER JOIN CCitadelPlayerPawn p \
                ON d.tick = p.tick \
             LIMIT 20"
                .to_string(),
        ];

        eprintln!("[test] Running event-entity JOIN query with LIMIT 100...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            total_rows += batch.num_rows();
            eprintln!("[test] Event-Entity JOIN batch: {} rows", batch.num_rows());
        }

        eprintln!("[test] Total event-entity JOIN results: {}", total_rows);
    }

    // =========================================================================
    // Full Scan Tests (no LIMIT)
    // =========================================================================

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_full_scan_damage_events() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_full_scan_damage_events_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_full_scan_damage_events_inner() {
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, damage, entindex_attacker, entindex_victim FROM DamageEvent LIMIT 1000"
                .to_string(),
        ];

        eprintln!("[test] Running damage events query...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();
            eprintln!(
                "[test] Progress: {} batches, {} rows",
                batch_count, total_rows
            );
        }

        eprintln!(
            "[test] Query complete: {} batches, {} total rows",
            batch_count, total_rows
        );
        assert!(total_rows > 0, "Should have damage events");
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_full_scan_with_nested_fields() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_full_scan_with_nested_fields_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_full_scan_with_nested_fields_inner() {
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec!["SELECT tick, damage, origin FROM DamageEvent LIMIT 1000".to_string()];

        eprintln!("[test] Running query with nested fields...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            total_rows += batch.num_rows();
        }

        eprintln!("[test] Query with nested fields: {} total rows", total_rows);
        assert!(total_rows > 0, "Should have damage events");
    }

    // =========================================================================
    // Complex Streaming Query Tests (JOINs + Window Functions)
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_complex_streaming_multi_table_join() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_complex_streaming_multi_table_join_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_complex_streaming_multi_table_join_inner() {
        let start = std::time::Instant::now();
        let mut first_batch_time = None;

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT \
                d.tick, \
                d.damage, \
                d.entindex_attacker, \
                d.entindex_victim, \
                p.entity_index as pawn_idx \
             FROM DamageEvent d \
             INNER JOIN CCitadelPlayerPawn p \
                ON d.tick = p.tick \
                AND d.entindex_attacker = p.entity_index \
             WHERE d.damage > 50 \
             LIMIT 100"
                .to_string(),
        ];

        eprintln!("[test] Running event-entity JOIN query...");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(64),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();

            if first_batch_time.is_none() {
                first_batch_time = Some(start.elapsed());
                eprintln!(
                    "[test] FIRST BATCH received at {:?} with {} rows",
                    first_batch_time.unwrap(),
                    batch.num_rows()
                );
            }
        }

        let total_time = start.elapsed();
        eprintln!(
            "[test] Query complete: {} batches, {} rows in {:?}",
            batch_count, total_rows, total_time
        );

        assert!(total_rows > 0, "Should have JOIN results");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_multi_event_join() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_multi_event_join_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_multi_event_join_inner() {
        let start = std::time::Instant::now();

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT \
                d.tick, \
                d.damage, \
                d.entindex_attacker, \
                d.entindex_victim as damage_victim, \
                k.entindex_victim as killed_victim \
             FROM DamageEvent d \
             INNER JOIN HeroKilledEvent k ON d.tick = k.tick \
             WHERE d.damage > 0 \
             LIMIT 100"
                .to_string(),
        ];

        eprintln!("[test] Running multi-event JOIN query...");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(32),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();

            if batch_count == 1 {
                eprintln!(
                    "[test] FIRST BATCH at {:?}: {} rows",
                    start.elapsed(),
                    batch.num_rows()
                );
                print_batch_sample(&batch, 5);
            }
        }

        let total_time = start.elapsed();
        eprintln!(
            "[test] Complete: {} batches, {} rows in {:?}",
            batch_count, total_rows, total_time
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_triple_table_join_entity_and_events() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_triple_table_join_entity_and_events_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_triple_table_join_entity_and_events_inner() {
        let start = std::time::Instant::now();

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT \
                d.tick, \
                d.damage, \
                d.entindex_attacker, \
                p.entity_index as attacker_pawn, \
                k.entindex_victim as killed_hero \
             FROM DamageEvent d \
             INNER JOIN CCitadelPlayerPawn p \
                ON d.tick = p.tick \
                AND d.entindex_attacker = p.entity_index \
             INNER JOIN HeroKilledEvent k \
                ON d.tick = k.tick \
             WHERE d.damage > 0 \
             LIMIT 30"
                .to_string(),
        ];

        eprintln!("[test] Running triple-table JOIN query...");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(32),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();

            if batch_count == 1 {
                eprintln!(
                    "[test] FIRST BATCH at {:?}: {} rows",
                    start.elapsed(),
                    batch.num_rows()
                );
                print_batch_sample(&batch, 5);
            }
        }

        let total_time = start.elapsed();
        eprintln!(
            "[test] Complete: {} batches, {} rows in {:?}",
            batch_count, total_rows, total_time
        );
    }

    fn print_batch_sample(batch: &RecordBatch, max_rows: usize) {
        eprintln!("[test] Sample rows (up to {}):", max_rows);
        let num_rows = batch.num_rows().min(max_rows);
        let schema = batch.schema();
        for row in 0..num_rows {
            let mut row_str = String::new();
            for (i, col) in batch.columns().iter().enumerate() {
                if i > 0 {
                    row_str.push_str(", ");
                }
                let field_name = schema.field(i).name();
                let value = datafusion::arrow::util::display::array_value_to_string(col, row)
                    .unwrap_or_else(|_| "?".to_string());
                row_str.push_str(&format!("{}={}", field_name, value));
            }
            eprintln!("[test]   Row {}: {}", row, row_str);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_simple_event_streaming() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_simple_event_streaming_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_simple_event_streaming_inner() {
        init_tracing();
        let start = std::time::Instant::now();

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, damage, entindex_attacker FROM DamageEvent WHERE damage > 0 LIMIT 100"
                .to_string(),
        ];

        eprintln!("[test] Running SIMPLE streaming query...");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128), // Testing with batch_size > expected events
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();

            if batch_count == 1 {
                eprintln!(
                    "[test] FIRST BATCH at {:?}: {} rows",
                    start.elapsed(),
                    batch.num_rows()
                );
            }
        }

        let total_time = start.elapsed();
        eprintln!(
            "[test] Complete: {} batches, {} rows in {:?}",
            batch_count, total_rows, total_time
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires test demo file"]
    async fn test_simple_event_streaming_single_thread() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(SINGLE_THREAD_TIMEOUT_SECS),
            test_simple_event_streaming_single_thread_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!(
                "[test] TIMEOUT after {} seconds",
                SINGLE_THREAD_TIMEOUT_SECS
            ),
        }
    }

    async fn test_simple_event_streaming_single_thread_inner() {
        let start = std::time::Instant::now();

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, damage, entindex_attacker FROM DamageEvent WHERE damage > 0 LIMIT 100"
                .to_string(),
        ];

        eprintln!("[test] Running SIMPLE streaming query (SINGLE THREAD)...");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(32),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();

            if batch_count == 1 {
                eprintln!(
                    "[test] FIRST BATCH at {:?}: {} rows",
                    start.elapsed(),
                    batch.num_rows()
                );
            }
        }

        let total_time = start.elapsed();
        eprintln!(
            "[test] Complete: {} batches, {} rows in {:?}",
            batch_count, total_rows, total_time
        );
    }

    // =========================================================================
    // UNION ALL Partition Regression Tests
    // =========================================================================
    //
    // These tests validate that UNION ALL queries return data from ALL branches.
    //
    // Previously, the code called `plan.execute(0, ctx)` which only executed
    // partition 0 of a multi-partition plan. For UNION ALL queries, this meant
    // only the first table's data was returned, causing:
    // - Memory leaks (unconsumed channel buffers)
    // - "Channel closed" errors (orphaned receivers)
    // - Silent data loss (missing rows from other UNION branches)
    //
    // We test both single-threaded and multi-threaded runtimes to ensure
    // the fix works regardless of executor configuration.

    const UNION_ALL_TIMEOUT_SECS: u64 = 60;
    const SINGLE_THREAD_TIMEOUT_SECS: u64 = 120;

    /// Shared test logic for UNION ALL entity table tests.
    /// Returns (total_rows, sources_seen) or panics on failure.
    async fn run_union_all_entity_test() -> (usize, std::collections::HashSet<String>) {
        eprintln!("[test] Loading demo bytes...");
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Loaded {} bytes", demo_bytes.len());

        eprintln!("[test] Discovering schemas...");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        eprintln!("[test] Discovered {} entity schemas", entity_schemas.len());

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // UNION ALL query with 3 different entity types, each tagged with a source identifier.
        // This is similar to the npcs.sql query that exposed the bug.
        //
        // Using tick < 1000 to limit data volume since filter pushdown isn't implemented.
        let query = r#"
            SELECT tick, entity_index, 'Pawn' as source_type
            FROM CCitadelPlayerPawn
            WHERE tick < 1000
            UNION ALL
            SELECT tick, entity_index, 'Controller' as source_type  
            FROM CCitadelPlayerController
            WHERE tick < 1000
            UNION ALL
            SELECT tick, entity_index, 'GameRules' as source_type
            FROM CCitadelGameRulesProxy
            WHERE tick < 1000
        "#
        .to_string();

        eprintln!("[test] Running query...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            vec![query],
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        assert_eq!(streams.len(), 1);
        let mut stream = streams.into_iter().next().unwrap();

        let mut sources_seen = std::collections::HashSet::new();
        let mut total_rows = 0;
        let mut batch_count = 0;

        eprintln!("[test] Consuming stream...");
        while let Some(result) = stream.next().await {
            let batch = result.expect("batch should succeed");
            batch_count += 1;
            total_rows += batch.num_rows();

            let source_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("source_type should be StringArray");

            for i in 0..source_col.len() {
                if let Some(source) = source_col.value(i).into() {
                    sources_seen.insert(source.to_string());
                }
            }

            if batch_count % 100 == 0 {
                eprintln!(
                    "[test] Batch {}: {} rows, sources: {:?}",
                    batch_count, total_rows, sources_seen
                );
            }
        }

        eprintln!(
            "[test] Complete: {} batches, {} rows, sources: {:?}",
            batch_count, total_rows, sources_seen
        );
        (total_rows, sources_seen)
    }

    fn assert_union_all_entity_results(
        total_rows: usize,
        sources_seen: &std::collections::HashSet<String>,
    ) {
        // CRITICAL: We must see data from ALL three UNION branches
        // With the old bug (execute partition 0 only), we would only see "Pawn"
        assert!(
            sources_seen.contains("Pawn"),
            "Should have data from CCitadelPlayerPawn (first UNION branch)"
        );
        assert!(
            sources_seen.contains("Controller"),
            "Should have data from CCitadelPlayerController (second UNION branch) - \
             this would fail with the old execute(0, ctx) bug!"
        );
        // GameRules may not exist in all demos, so we don't require it
        // but we verify we got data from at least 2 different partitions
        assert!(
            sources_seen.len() >= 2,
            "Should have data from at least 2 UNION branches, got: {:?}",
            sources_seen
        );
        assert!(total_rows > 0, "Should have some rows");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_union_all_entity_tables_multi_threaded() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(UNION_ALL_TIMEOUT_SECS),
            run_union_all_entity_test(),
        )
        .await;

        match result {
            Ok((total_rows, sources_seen)) => {
                assert_union_all_entity_results(total_rows, &sources_seen);
                eprintln!("[test] PASSED: {} rows from {:?}", total_rows, sources_seen);
            }
            Err(_) => panic!("[test] TIMEOUT after {} seconds", UNION_ALL_TIMEOUT_SECS),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires test demo file"]
    async fn test_union_all_entity_tables_single_threaded() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(SINGLE_THREAD_TIMEOUT_SECS),
            run_union_all_entity_test(),
        )
        .await;

        match result {
            Ok((total_rows, sources_seen)) => {
                assert_union_all_entity_results(total_rows, &sources_seen);
                eprintln!("[test] PASSED: {} rows from {:?}", total_rows, sources_seen);
            }
            Err(_) => panic!(
                "[test] TIMEOUT after {} seconds",
                SINGLE_THREAD_TIMEOUT_SECS
            ),
        }
    }

    /// Shared test logic for UNION ALL event table tests.
    async fn run_union_all_event_test() -> (usize, std::collections::HashSet<String>) {
        eprintln!("[test] Loading demo bytes...");
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Loaded {} bytes", demo_bytes.len());

        eprintln!("[test] Discovering schemas...");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        eprintln!("[test] Discovered {} entity schemas", entity_schemas.len());

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // UNION ALL with event tables - different event types
        let query = r#"
            SELECT tick, 'Damage' as event_type FROM DamageEvent WHERE tick < 10000
            UNION ALL
            SELECT tick, 'BulletHit' as event_type FROM BulletHitEvent WHERE tick < 10000
            UNION ALL
            SELECT tick, 'HeroKilled' as event_type FROM HeroKilledEvent WHERE tick < 10000
        "#
        .to_string();

        eprintln!("[test] Running query...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            vec![query],
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_streams();

        assert_eq!(streams.len(), 1);
        let mut stream = streams.into_iter().next().unwrap();

        let mut event_types_seen = std::collections::HashSet::new();
        let mut total_rows = 0;
        let mut batch_count = 0;

        eprintln!("[test] Consuming stream...");
        while let Some(result) = stream.next().await {
            let batch = result.expect("batch should succeed");
            batch_count += 1;
            total_rows += batch.num_rows();

            let event_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .expect("event_type should be StringArray");

            for i in 0..event_col.len() {
                if let Some(event_type) = event_col.value(i).into() {
                    event_types_seen.insert(event_type.to_string());
                }
            }

            if batch_count % 100 == 0 {
                eprintln!(
                    "[test] Batch {}: {} rows, event types: {:?}",
                    batch_count, total_rows, event_types_seen
                );
            }
        }

        eprintln!(
            "[test] Complete: {} batches, {} rows, event types: {:?}",
            batch_count, total_rows, event_types_seen
        );
        (total_rows, event_types_seen)
    }

    fn assert_union_all_event_results(
        total_rows: usize,
        event_types_seen: &std::collections::HashSet<String>,
    ) {
        // We should see data from multiple event types
        assert!(
            event_types_seen.len() >= 2,
            "Should have data from at least 2 event types in UNION ALL, got: {:?}",
            event_types_seen
        );
        assert!(total_rows > 0, "Should have some rows");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_union_all_event_tables_multi_threaded() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(UNION_ALL_TIMEOUT_SECS),
            run_union_all_event_test(),
        )
        .await;

        match result {
            Ok((total_rows, event_types_seen)) => {
                assert_union_all_event_results(total_rows, &event_types_seen);
                eprintln!(
                    "[test] PASSED: {} rows from {:?}",
                    total_rows, event_types_seen
                );
            }
            Err(_) => panic!("[test] TIMEOUT after {} seconds", UNION_ALL_TIMEOUT_SECS),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_entity_join_streaming_timing() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            test_entity_join_streaming_timing_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 60 seconds"),
        }
    }

    async fn test_entity_join_streaming_timing_inner() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let start = std::time::Instant::now();

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Demo loaded at {:?}", start.elapsed());

        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT d.tick, d.damage, k.entindex_victim as killed \
             FROM DamageEvent d \
             INNER JOIN HeroKilledEvent k ON d.tick = k.tick \
             WHERE d.damage > 0"
                .to_string(),
        ];

        eprintln!("[test] Running JOIN query at {:?}...", start.elapsed());
        let (streams, parser_handle) = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries")
        .into_parts();
        eprintln!("[test] Streams created at {:?}", start.elapsed());

        let parser_finished = Arc::new(AtomicBool::new(false));
        let parser_finished_clone = Arc::clone(&parser_finished);
        let parser_monitor = tokio::spawn(async move {
            let result = parser_handle.await;
            parser_finished_clone.store(true, Ordering::SeqCst);
            result
        });

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;
        let mut first_batch_time = None;
        let mut first_batch_before_parser_finished = false;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();

            if first_batch_time.is_none() {
                first_batch_time = Some(start.elapsed());
                first_batch_before_parser_finished = !parser_finished.load(Ordering::SeqCst);
                eprintln!(
                    "[test] FIRST BATCH at {:?}: {} rows (parser_finished={})",
                    first_batch_time.unwrap(),
                    batch.num_rows(),
                    !first_batch_before_parser_finished
                );
            }
        }

        let _ = parser_monitor.await;

        let total_time = start.elapsed();
        eprintln!(
            "[test] Total: {} batches, {} rows in {:?}",
            batch_count, total_rows, total_time
        );

        if let Some(first) = first_batch_time {
            eprintln!("[test] Time to first batch: {:?}", first);
            eprintln!(
                "[test] First batch arrived before parser finished: {}",
                first_batch_before_parser_finished
            );

            assert!(
                first.as_secs() < 5,
                "First batch took {:?} - expected < 5s for streaming JOIN.",
                first
            );

            assert!(
                first_batch_before_parser_finished,
                "First batch arrived AFTER parser finished - streaming may be broken."
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires test demo file"]
    async fn test_union_all_event_tables_single_threaded() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(SINGLE_THREAD_TIMEOUT_SECS),
            run_union_all_event_test(),
        )
        .await;

        match result {
            Ok((total_rows, event_types_seen)) => {
                assert_union_all_event_results(total_rows, &event_types_seen);
                eprintln!(
                    "[test] PASSED: {} rows from {:?}",
                    total_rows, event_types_seen
                );
            }
            Err(_) => panic!(
                "[test] TIMEOUT after {} seconds",
                SINGLE_THREAD_TIMEOUT_SECS
            ),
        }
    }
}
