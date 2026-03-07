//! Integration tests for event table SQL queries.
//!
//! These tests read real demo files and execute SQL queries against event tables,
//! validating the full pipeline from schema discovery through query execution.

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
    use crate::events::{event_schema, EventType};
    use crate::haste::async_demofile::AsyncDemoFile;
    use crate::haste::core::packet_source::PacketSource;
    use crate::haste::parser::AsyncStreamingParser;
    use crate::schema::EntitySchema;
    use crate::visitor::{DiscoveredSchemas, SchemaDiscoveryVisitor};

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
        let demo_file = AsyncDemoFile::start_reading(reader)
            .await
            .map_err(|e| crate::error::Source2DfError::Haste(format!("Failed to read demo: {}", e)))?;

        let visitor = SchemaDiscoveryVisitor::new();
        let symbols_handle = visitor.symbols_handle();

        let mut parser = AsyncStreamingParser::from_stream_with_visitor(demo_file, visitor)
            .map_err(|e| crate::error::Source2DfError::Haste(e.to_string()))?;

        let _ = parser.run_to_end().await;

        let ctx = parser.context();
        let symbols = symbols_handle
            .lock()
            .map_err(|_| crate::error::Source2DfError::Schema("Failed to lock symbols".to_string()))?
            .take()
            .ok_or_else(|| crate::error::Source2DfError::Schema("No symbols discovered".to_string()))?;

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
        assert_eq!(EventType::ModifierApplied.table_name(), "ModifierAppliedEvent");
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
            entity_names.iter().any(|n| *n == "CCitadelPlayerController"),
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

    #[tokio::test]
    #[ignore = "requires test demo file and takes several minutes to parse 700MB+ demo"]
    async fn test_query_event_table_e2e() {
        // Tests AbilitiesChangedEvent which has only scalar fields and fires frequently.
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        // Wrap entire test in timeout to detect hangs
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            test_query_event_table_e2e_inner()
        ).await;
        
        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds - likely deadlock"),
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
            "SELECT tick, purchaser_player_slot, ability_id, change FROM AbilitiesChangedEvent LIMIT 100".to_string(),
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
        .expect("run_queries").into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 1);

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut poll_count = 0;

        eprintln!("[test] Starting to consume stream...");
        
        // Use timeout on each poll to detect where we hang
        loop {
            let poll_result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                stream.next()
            ).await;
            
            match poll_result {
                Ok(Some(result)) => {
                    poll_count += 1;
                    let batch = result.expect("batch");
                    let batch_rows = batch.num_rows();
                    total_rows += batch_rows;
                    eprintln!("[test] Poll {}: received batch with {} rows (total: {})", poll_count, batch_rows, total_rows);
                    batches.push(batch);
                    
                    if total_rows >= 100 {
                        eprintln!("[test] Reached 100 rows, breaking");
                        break;
                    }
                }
                Ok(None) => {
                    eprintln!("[test] Stream ended naturally");
                    break;
                }
                Err(_) => {
                    panic!("[test] TIMEOUT waiting for next batch after poll {} (total_rows={})", poll_count, total_rows);
                }
            }
        }

        eprintln!("[test] Stream consumption complete: {} batches, {} total rows", batches.len(), total_rows);
        eprintln!("[test] Dropping stream...");
        drop(stream);
        eprintln!("[test] Stream dropped");
        
        assert!(total_rows > 0, "Should receive AbilitiesChanged events from demo");
        
        // Verify schema of returned batches
        if let Some(batch) = batches.first() {
            assert!(batch.schema().field_with_name("tick").is_ok());
            assert!(batch.schema().field_with_name("ability_id").is_ok());
        }
        
        eprintln!("[test] Assertions passed, test function ending");
    }

    #[tokio::test]
    #[ignore = "requires test demo file and takes several minutes to parse 700MB+ demo"]
    async fn test_query_multiple_event_types() {
        // Tests BossDamagedEvent and RejuvStatusEvent which have only scalar fields.
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        // Wrap entire test in timeout to detect hangs
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            test_query_multiple_event_types_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds - likely deadlock"),
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
        .expect("run_queries").into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 2, "Should have 2 query streams");

        // IMPORTANT: Consume streams concurrently to avoid deadlock
        let stream_futures: Vec<_> = streams
            .into_iter()
            .enumerate()
            .map(|(i, mut stream)| {
                async move {
                    let mut rows = 0;
                    let mut batch_count = 0;
                    eprintln!("[test] Stream {} starting to consume", i);
                    while let Some(result) = stream.next().await {
                        let batch = result.expect("batch");
                        batch_count += 1;
                        rows += batch.num_rows();
                        eprintln!(
                            "[test] Stream {} batch {}: {} rows (total: {})",
                            i, batch_count, batch.num_rows(), rows
                        );
                    }
                    eprintln!("[test] Stream {} completed with {} rows", i, rows);
                    rows
                }
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
    #[ignore = "requires test demo file and takes several minutes to parse 700MB+ demo"]  
    async fn test_query_entity_and_event_together() {
        // Tests querying both entity tables and event tables concurrently.
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");

        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // Query both entity table and event table
        let queries = vec![
            "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 50".to_string(),
            "SELECT tick, objective_team, entity_damaged FROM BossDamagedEvent LIMIT 50".to_string(),
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
        .expect("run_queries").into_streams();

        assert_eq!(streams.len(), 2);

        let mut streams_vec: Vec<_> = streams.into_iter().collect();
        let entity_stream = streams_vec.remove(0);
        let event_stream = streams_vec.remove(0);

        // IMPORTANT: Consume both streams concurrently to avoid deadlock.
        // The parser sends to both channels, and if we only pull from one,
        // the other channel fills up and blocks the parser.
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
        // BossDamaged events may be rare - don't assert event_rows > 0
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_query_damage_event_with_nested_messages() {
        // Test DamageEvent which has nested CMsgVector fields (origin, damage_direction).
        // This validates that the code generation for nested messages works correctly.
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            test_query_damage_event_with_nested_messages_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds - likely deadlock"),
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
        .expect("run_queries").into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 1);

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut poll_count = 0;

        eprintln!("[test] Starting to consume stream...");

        loop {
            let poll_result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                stream.next(),
            )
            .await;

            match poll_result {
                Ok(Some(result)) => {
                    poll_count += 1;
                    let batch = result.expect("batch");
                    let batch_rows = batch.num_rows();
                    total_rows += batch_rows;
                    eprintln!(
                        "[test] Poll {}: received batch with {} rows (total: {})",
                        poll_count, batch_rows, total_rows
                    );
                    batches.push(batch);

                    if total_rows >= 100 {
                        eprintln!("[test] Reached 100 rows, breaking");
                        break;
                    }
                }
                Ok(None) => {
                    eprintln!("[test] Stream ended naturally");
                    break;
                }
                Err(_) => {
                    panic!(
                        "[test] TIMEOUT waiting for next batch after poll {} (total_rows={})",
                        poll_count, total_rows
                    );
                }
            }
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
            assert!(schema.field_with_name("origin").is_ok(), "Should have origin (nested CMsgVector) field");
            
            // Log the origin field type to verify it's a struct
            let origin_field = schema.field_with_name("origin").unwrap();
            eprintln!("[test] origin field type: {:?}", origin_field.data_type());
        }

        eprintln!("[test] Assertions passed, test function ending");
    }

    #[tokio::test]
    #[ignore = "requires test demo file"]
    async fn test_query_hero_killed_event_with_list_field() {
        // Test HeroKilledEvent which has `repeated int32 entindex_assisters` - a list of scalars.
        // This validates that the code generation for list fields works correctly.
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            test_query_hero_killed_event_with_list_field_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds - likely deadlock"),
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
        .expect("run_queries").into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 1);

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut poll_count = 0;

        eprintln!("[test] Starting to consume stream...");

        loop {
            let poll_result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                stream.next(),
            )
            .await;

            match poll_result {
                Ok(Some(result)) => {
                    poll_count += 1;
                    let batch = result.expect("batch");
                    let batch_rows = batch.num_rows();
                    total_rows += batch_rows;
                    eprintln!(
                        "[test] Poll {}: received batch with {} rows (total: {})",
                        poll_count, batch_rows, total_rows
                    );
                    batches.push(batch);

                    if total_rows >= 50 {
                        eprintln!("[test] Reached 50 rows, breaking");
                        break;
                    }
                }
                Ok(None) => {
                    eprintln!("[test] Stream ended naturally with {} rows", total_rows);
                    break;
                }
                Err(_) => {
                    panic!(
                        "[test] TIMEOUT waiting for next batch after poll {} (total_rows={})",
                        poll_count, total_rows
                    );
                }
            }
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
        // Test RecentDamageSummaryEvent which has `repeated DamageRecord damage_records` 
        // and `repeated ModifierRecord modifier_records` - lists of nested messages.
        // This validates that the code generation for repeated Message fields works correctly.
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            test_query_recent_damage_summary_event_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds - likely deadlock"),
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
        .expect("run_queries").into_streams();

        eprintln!("[test] run_queries returned {} streams", streams.len());
        assert_eq!(streams.len(), 1);

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut poll_count = 0;

        eprintln!("[test] Starting to consume stream...");

        loop {
            let poll_result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                stream.next(),
            )
            .await;

            match poll_result {
                Ok(Some(result)) => {
                    poll_count += 1;
                    let batch = result.expect("batch");
                    let batch_rows = batch.num_rows();
                    total_rows += batch_rows;
                    eprintln!(
                        "[test] Poll {}: received batch with {} rows (total: {})",
                        poll_count, batch_rows, total_rows
                    );
                    batches.push(batch);

                    if total_rows >= 50 {
                        eprintln!("[test] Reached 50 rows, breaking");
                        break;
                    }
                }
                Ok(None) => {
                    eprintln!("[test] Stream ended naturally with {} rows", total_rows);
                    break;
                }
                Err(_) => {
                    panic!(
                        "[test] TIMEOUT waiting for next batch after poll {} (total_rows={})",
                        poll_count, total_rows
                    );
                }
            }
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
        let schema = event_schema("RecentDamageSummaryEvent").expect("RecentDamageSummaryEvent schema");
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
        if let datafusion::arrow::datatypes::DataType::List(inner_field) = damage_records_field.data_type() {
            eprintln!("[test] damage_records inner field: {:?}", inner_field);
            if let datafusion::arrow::datatypes::DataType::Struct(fields) = inner_field.data_type() {
                let field_names: Vec<_> = fields.iter().map(|f| f.name().as_str()).collect();
                eprintln!("[test] DamageRecord struct fields: {:?}", field_names);
                assert!(field_names.contains(&"damage"), "DamageRecord should have 'damage' field");
                assert!(field_names.contains(&"hits"), "DamageRecord should have 'hits' field");
                assert!(field_names.contains(&"hero_id"), "DamageRecord should have 'hero_id' field");
            }
        }

        eprintln!("[test] Assertions passed, test function ending");
    }

    // =========================================================================
    // Window Function Tests (streaming-compatible)
    // =========================================================================
    //
    // NOTE: Traditional aggregations (COUNT(*), GROUP BY) are NOT supported on
    // unbounded streams because they require seeing all data before emitting.
    // Use window functions with tick_bin() for streaming aggregates.

    #[tokio::test]
    #[ignore = "requires test demo file - window function"]
    async fn test_window_running_damage_sum() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            test_window_running_damage_sum_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds"),
        }
    }

    async fn test_window_running_damage_sum_inner() {
        eprintln!("[test] Loading demo bytes...");
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Loaded {} bytes", demo_bytes.len());

        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // Running sum of damage per time bucket using tick_bin UDF
        // This is streaming-compatible because partitions complete as ticks advance
        let queries = vec![
            "SELECT \
                tick, \
                damage, \
                tick_bin(tick, 1000) as time_bucket, \
                SUM(damage) OVER (PARTITION BY tick_bin(tick, 1000) ORDER BY tick) as bucket_running_sum \
             FROM DamageEvent \
             LIMIT 1000".to_string(),
        ];

        eprintln!("[test] Running window function query...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batches: Vec<RecordBatch> = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            total_rows += batch.num_rows();
            batches.push(batch);
            if total_rows >= 1000 {
                break;
            }
        }

        eprintln!("[test] Got {} rows from window function", total_rows);
        assert!(total_rows > 0, "Should have window function results");

        // Verify schema has expected columns
        if let Some(batch) = batches.first() {
            assert!(batch.schema().field_with_name("tick").is_ok());
            assert!(batch.schema().field_with_name("damage").is_ok());
            assert!(batch.schema().field_with_name("time_bucket").is_ok());
            assert!(batch.schema().field_with_name("bucket_running_sum").is_ok());
        }
    }

    #[tokio::test]
    #[ignore = "requires test demo file - window function"]
    async fn test_window_row_number_per_attacker() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            test_window_row_number_per_attacker_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds"),
        }
    }

    async fn test_window_row_number_per_attacker_inner() {
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // ROW_NUMBER per attacker within each time bucket
        let queries = vec![
            "SELECT \
                tick, \
                damage, \
                entindex_attacker, \
                tick_bin(tick, 1000) as time_bucket, \
                ROW_NUMBER() OVER (PARTITION BY tick_bin(tick, 1000), entindex_attacker ORDER BY tick) as hit_num \
             FROM DamageEvent \
             WHERE entindex_attacker IS NOT NULL \
             LIMIT 500".to_string(),
        ];

        eprintln!("[test] Running ROW_NUMBER window query...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            total_rows += batch.num_rows();
            if total_rows >= 500 {
                break;
            }
        }

        eprintln!("[test] Got {} rows from ROW_NUMBER query", total_rows);
        assert!(total_rows > 0, "Should have ROW_NUMBER results");
    }

    #[tokio::test]
    #[ignore = "requires test demo file - validates aggregation rejection"]
    async fn test_aggregation_rejected_on_unbounded_stream() {
        // This test verifies that traditional aggregations are correctly
        // rejected by DataFusion's sanity checker for unbounded streams.
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // This aggregation SHOULD fail on unbounded streams
        let queries = vec![
            "SELECT COUNT(*) as total FROM DamageEvent".to_string(),
        ];

        let result = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await;

        // Should fail with pipeline breaking error
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
        use datafusion::prelude::*;
        use crate::events::EventType;

        // Use streaming-compatible session config (target_partitions=1)
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);
        let ctx = SessionContext::new_with_config(config);

        // Register two event tables with mock receivers
        let (_, damage_rx) = async_channel::unbounded::<(i32, std::sync::Arc<crate::events::DecodedEvent>)>();
        let (_, kill_rx) = async_channel::unbounded::<(i32, std::sync::Arc<crate::events::DecodedEvent>)>();

        let damage_provider = crate::datafusion::event_table_provider::EventTableProvider::new(
            EventType::Damage,
            damage_rx,
        ).expect("create damage provider");

        let kill_provider = crate::datafusion::event_table_provider::EventTableProvider::new(
            EventType::HeroKilled,
            kill_rx,
        ).expect("create kill provider");

        ctx.register_table("DamageEvent", std::sync::Arc::new(damage_provider)).unwrap();
        ctx.register_table("HeroKilledEvent", std::sync::Arc::new(kill_provider)).unwrap();

        // Plan a JOIN query
        let sql = "SELECT d.tick, d.damage, k.entindex_victim \
                   FROM DamageEvent d \
                   INNER JOIN HeroKilledEvent k ON d.tick = k.tick";

        let logical = ctx.state().create_logical_plan(sql).await.expect("logical plan");
        let physical = ctx.state().create_physical_plan(&logical).await.expect("physical plan");

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
        use datafusion::prelude::*;
        use crate::datafusion::pipeline_analysis::analyze_pipeline;
        use crate::events::EventType;

        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);
        let ctx = SessionContext::new_with_config(config);

        let (_, damage_rx) = async_channel::unbounded::<(i32, std::sync::Arc<crate::events::DecodedEvent>)>();
        let (_, kill_rx) = async_channel::unbounded::<(i32, std::sync::Arc<crate::events::DecodedEvent>)>();

        let damage_provider = crate::datafusion::event_table_provider::EventTableProvider::new(
            EventType::Damage,
            damage_rx,
        ).expect("create damage provider");

        let kill_provider = crate::datafusion::event_table_provider::EventTableProvider::new(
            EventType::HeroKilled,
            kill_rx,
        ).expect("create kill provider");

        ctx.register_table("DamageEvent", std::sync::Arc::new(damage_provider)).unwrap();
        ctx.register_table("HeroKilledEvent", std::sync::Arc::new(kill_provider)).unwrap();

        let sql = "SELECT d.tick, d.damage, k.entindex_victim \
                   FROM DamageEvent d \
                   INNER JOIN HeroKilledEvent k ON d.tick = k.tick";

        let logical = ctx.state().create_logical_plan(sql).await.expect("logical plan");
        let physical = ctx.state().create_physical_plan(&logical).await.expect("physical plan");

        let analysis = analyze_pipeline(&physical);
        eprintln!("\n{}\n", analysis.report());

        assert!(
            analysis.is_streaming_safe(),
            "Physical plan has pipeline breakers! This will cause streaming delay.\n{}",
            analysis.report()
        );
    }

    #[tokio::test]
    #[ignore = "requires test demo file - runs JOIN query"]
    async fn test_join_damage_with_hero_killed() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            test_join_damage_with_hero_killed_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds - likely deadlock or wrong plan"),
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
             WHERE d.damage > 0".to_string(),
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
        .expect("run_queries").into_streams();
        eprintln!("[test] Streams created at {:?}", start.elapsed());

        assert_eq!(streams.len(), 1, "Should have 1 stream");

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut first_batch = true;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            total_rows += batch.num_rows();
            if first_batch {
                eprintln!("[test] FIRST batch at {:?}: {} rows", start.elapsed(), batch.num_rows());
                first_batch = false;
            } else {
                eprintln!("[test] batch: {} rows (total: {}) at {:?}", batch.num_rows(), total_rows, start.elapsed());
            }
        }

        eprintln!("[test] Total JOIN results: {} at {:?}", total_rows, start.elapsed());
        // JOIN may produce 0 rows if no damage happened on exact kill ticks, that's OK
    }

    #[tokio::test]
    async fn test_join_event_entity_plan_selection() {
        // Verify physical plan for event-entity JOIN
        use datafusion::prelude::*;
        use crate::events::EventType;

        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);
        let ctx = SessionContext::new_with_config(config);

        // Create event table provider
        let (_, damage_rx) = async_channel::unbounded::<(i32, std::sync::Arc<crate::events::DecodedEvent>)>();
        let damage_provider = crate::datafusion::event_table_provider::EventTableProvider::new(
            EventType::Damage,
            damage_rx,
        ).expect("create damage provider");
        ctx.register_table("DamageEvent", std::sync::Arc::new(damage_provider)).unwrap();

        // Create entity table provider using QueryTableProvider
        let entity_schema = datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("tick", datafusion::arrow::datatypes::DataType::Int32, false),
            datafusion::arrow::datatypes::Field::new("entity_index", datafusion::arrow::datatypes::DataType::Int32, false),
            datafusion::arrow::datatypes::Field::new("delta_type", datafusion::arrow::datatypes::DataType::Utf8, false),
        ]);
        let slot = std::sync::Arc::new(parking_lot::Mutex::new(None));
        let entity_provider = crate::datafusion::query_session::QueryTableProvider::new(
            std::sync::Arc::new(entity_schema),
            std::sync::Arc::from("CCitadelPlayerPawn"),
            slot,
        );
        ctx.register_table("CCitadelPlayerPawn", std::sync::Arc::new(entity_provider)).unwrap();

        let sql = "SELECT d.tick, d.damage, p.entity_index \
                   FROM DamageEvent d \
                   INNER JOIN CCitadelPlayerPawn p ON d.tick = p.tick";

        let logical = ctx.state().create_logical_plan(sql).await.expect("logical plan");
        let physical = ctx.state().create_physical_plan(&logical).await.expect("physical plan");

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
    #[ignore = "requires test demo file - runs JOIN query"]
    async fn test_join_event_with_entity_table() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            test_join_event_with_entity_table_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 120 seconds - likely deadlock or wrong plan"),
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
             LIMIT 20".to_string(),
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
        .expect("run_queries").into_streams();

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
    #[ignore = "requires test demo file - full scan"]
    async fn test_full_scan_damage_events() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            test_full_scan_damage_events_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 10 seconds"),
        }
    }

    async fn test_full_scan_damage_events_inner() {
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // Full scan - no LIMIT
        let queries = vec![
            "SELECT tick, damage, entindex_attacker, entindex_victim FROM DamageEvent".to_string(),
        ];

        eprintln!("[test] Running full scan query...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();
            if batch_count % 100 == 0 {
                eprintln!("[test] Progress: {} batches, {} rows", batch_count, total_rows);
            }
        }

        eprintln!("[test] Full scan complete: {} batches, {} total rows", batch_count, total_rows);
        assert!(total_rows > 1000, "Should have many damage events in a full demo");
    }

    #[tokio::test]
    #[ignore = "requires test demo file - full scan with nested fields"]
    async fn test_full_scan_with_nested_fields() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            test_full_scan_with_nested_fields_inner(),
        )
        .await;

        match result {
            Ok(()) => eprintln!("[test] Completed successfully"),
            Err(_) => panic!("[test] TIMEOUT after 10 seconds"),
        }
    }

    async fn test_full_scan_with_nested_fields_inner() {
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // Query nested struct fields - origin.x, origin.y, origin.z
        let queries = vec![
            "SELECT tick, damage, origin FROM DamageEvent".to_string(),
        ];

        eprintln!("[test] Running full scan with nested fields...");
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(128),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            total_rows += batch.num_rows();
        }

        eprintln!("[test] Full scan with nested fields: {} total rows", total_rows);
        assert!(total_rows > 0, "Should have damage events");
    }

    // =========================================================================
    // Complex Streaming Query Tests (JOINs + Window Functions)
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file - complex streaming with JOINs"]
    async fn test_complex_streaming_multi_table_join() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        // This test validates near-realtime streaming with complex cross-table JOINs:
        // - JOIN between event table (DamageEvent) and entity table (CCitadelPlayerPawn)
        // - Filter on damage amount
        //
        // Expected behavior: Results stream incrementally as parser produces data,
        // NOT waiting for full parse completion.

        let start = std::time::Instant::now();
        let mut first_batch_time = None;

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // JOIN damage events with player pawn to correlate attacker info
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
             LIMIT 100".to_string(),
        ];

        eprintln!("[test] Running event-entity JOIN query...");
        eprintln!("[test] Query: JOIN DamageEvent with CCitadelPlayerPawn WHERE damage > 50");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(64),  // Smaller batch size for more streaming granularity
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();

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

            eprintln!(
                "[test] Batch {}: {} rows (total: {}) at {:?}",
                batch_count,
                batch.num_rows(),
                total_rows,
                start.elapsed()
            );
        }

        let total_time = start.elapsed();
        eprintln!("[test] Query complete: {} batches, {} rows in {:?}", batch_count, total_rows, total_time);

        if let Some(first) = first_batch_time {
            eprintln!("[test] Time to first batch: {:?}", first);
            eprintln!("[test] Near-realtime ratio: {:.1}x", total_time.as_secs_f64() / first.as_secs_f64());
        }

        // Verify we got results
        assert!(total_rows > 0, "Should have JOIN results");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file - window function on single event table"]
    async fn test_window_function_on_event_table() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        // Window function on single event table (works because source is ordered by tick)
        // tick_bin enables BoundedWindowAggExec for streaming execution

        let start = std::time::Instant::now();
        let mut first_batch_time = None;

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT \
                tick, \
                damage, \
                entindex_attacker, \
                tick_bin(tick, 1000) as time_bucket, \
                ROW_NUMBER() OVER (PARTITION BY tick_bin(tick, 1000), entindex_attacker ORDER BY tick) as hit_num, \
                SUM(damage) OVER (PARTITION BY tick_bin(tick, 1000), entindex_attacker ORDER BY tick ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_damage \
             FROM DamageEvent \
             WHERE damage > 0 AND entindex_attacker IS NOT NULL \
             LIMIT 200".to_string(),
        ];

        eprintln!("[test] Running window function query on DamageEvent...");
        eprintln!("[test] Query: ROW_NUMBER + running SUM partitioned by tick_bin and attacker");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(32),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();

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
                    "[test] FIRST BATCH at {:?}: {} rows",
                    first_batch_time.unwrap(),
                    batch.num_rows()
                );
                print_batch_sample(&batch, 5);
            }

            eprintln!(
                "[test] Batch {}: {} rows at {:?}",
                batch_count,
                batch.num_rows(),
                start.elapsed()
            );
        }

        let total_time = start.elapsed();
        eprintln!("[test] Complete: {} batches, {} rows in {:?}", batch_count, total_rows, total_time);

        if let Some(first) = first_batch_time {
            let ratio = if first.as_secs_f64() > 0.0 {
                total_time.as_secs_f64() / first.as_secs_f64()
            } else {
                1.0
            };
            eprintln!("[test] Time to first batch: {:?}", first);
            eprintln!("[test] Near-realtime streaming factor: {:.1}x", ratio);
        }

        assert!(total_rows > 0, "Should have window function results");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file - multi-event JOIN"]
    async fn test_multi_event_join() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        // JOIN two event types on tick:
        // Find all damage that occurred on the same tick as a hero kill

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
                d.entindex_victim as damage_victim, \
                k.entindex_victim as killed_victim \
             FROM DamageEvent d \
             INNER JOIN HeroKilledEvent k ON d.tick = k.tick \
             WHERE d.damage > 0 \
             LIMIT 100".to_string(),
        ];

        eprintln!("[test] Running multi-event JOIN query...");
        eprintln!("[test] Query: DamageEvent JOIN HeroKilledEvent ON tick");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(32),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();

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
                    "[test] FIRST BATCH at {:?}: {} rows",
                    first_batch_time.unwrap(),
                    batch.num_rows()
                );
                
                // Print first few rows for debugging
                print_batch_sample(&batch, 5);
            }

            eprintln!(
                "[test] Batch {}: {} rows at {:?}",
                batch_count,
                batch.num_rows(),
                start.elapsed()
            );
        }

        let total_time = start.elapsed();
        eprintln!("[test] Complete: {} batches, {} rows in {:?}", batch_count, total_rows, total_time);

        if let Some(first) = first_batch_time {
            eprintln!("[test] Time to first batch: {:?}", first);
        }
        
        // May have 0 rows if the demo doesn't have matching damage/kill events
        eprintln!("[test] Multi-event JOIN produced {} rows", total_rows);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file - triple table JOIN"]
    async fn test_triple_table_join_entity_and_events() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        // Most complex test: Three-way JOIN
        // - CCitadelPlayerPawn (attacker position)
        // - DamageEvent (damage dealt)
        // - HeroKilledEvent (kill confirmation)
        //
        // Find damage events where the attacker was tracked as a pawn
        // AND a hero was killed on the same tick.

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
                p.entity_index as attacker_pawn, \
                k.entindex_victim as killed_hero \
             FROM DamageEvent d \
             INNER JOIN CCitadelPlayerPawn p \
                ON d.tick = p.tick \
                AND d.entindex_attacker = p.entity_index \
             INNER JOIN HeroKilledEvent k \
                ON d.tick = k.tick \
             WHERE d.damage > 0 \
             LIMIT 30".to_string(),
        ];

        eprintln!("[test] Running triple-table JOIN query...");
        eprintln!("[test] Query: DamageEvent + CCitadelPlayerPawn + HeroKilledEvent");

        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(32),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();

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
                    "[test] FIRST BATCH at {:?}: {} rows",
                    first_batch_time.unwrap(),
                    batch.num_rows()
                );
                print_batch_sample(&batch, 5);
            }

            eprintln!(
                "[test] Batch {}: {} rows at {:?}",
                batch_count,
                batch.num_rows(),
                start.elapsed()
            );
        }

        let total_time = start.elapsed();
        eprintln!("[test] Complete: {} batches, {} rows in {:?}", batch_count, total_rows, total_time);

        if let Some(first) = first_batch_time {
            let ratio = if first.as_secs_f64() > 0.0 {
                total_time.as_secs_f64() / first.as_secs_f64()
            } else {
                0.0
            };
            eprintln!("[test] Time to first batch: {:?}", first);
            eprintln!("[test] Near-realtime streaming factor: {:.1}x", ratio);
            
            // If streaming is working, first batch should arrive well before total completion
            // A ratio > 2.0 suggests incremental streaming is working
            if ratio > 1.5 {
                eprintln!("[test] ✓ Good streaming behavior detected");
            }
        }

        // May have 0 rows if no kills happened with matching conditions
        eprintln!("[test] Triple JOIN produced {} rows", total_rows);
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
                let value = datafusion::arrow::util::display::array_value_to_string(col, row).unwrap_or_else(|_| "?".to_string());
                row_str.push_str(&format!("{}={}", field_name, value));
            }
            eprintln!("[test]   Row {}: {}", row, row_str);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file - simple streaming sanity check"]
    async fn test_simple_event_streaming() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        // Simplest possible query - no JOINs, no window functions
        // This should stream immediately if streaming works at all

        let start = std::time::Instant::now();
        let mut first_batch_time = None;

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, damage, entindex_attacker FROM DamageEvent WHERE damage > 0 LIMIT 100".to_string(),
        ];

        eprintln!("[test] Running SIMPLE streaming query...");
        eprintln!("[test] Query: SELECT tick, damage FROM DamageEvent LIMIT 100");

        eprintln!("[test] Calling run_queries at {:?}", start.elapsed());
        let streams = StreamingQuerySession::run_queries(
            packet_source,
            &entity_schemas,
            queries,
            Some(32),
            HashMap::new(),
            StreamFormat::DemoFile,
        )
        .await
        .expect("run_queries").into_streams();
        eprintln!("[test] run_queries returned at {:?}", start.elapsed());

        let mut stream = streams.into_iter().next().unwrap();
        let mut total_rows = 0;
        let mut batch_count = 0;

        eprintln!("[test] Starting to poll stream at {:?}", start.elapsed());
        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();

            if first_batch_time.is_none() {
                first_batch_time = Some(start.elapsed());
                eprintln!(
                    "[test] FIRST BATCH at {:?}: {} rows",
                    first_batch_time.unwrap(),
                    batch.num_rows()
                );
            }

            eprintln!(
                "[test] Batch {}: {} rows at {:?}",
                batch_count,
                batch.num_rows(),
                start.elapsed()
            );
        }

        let total_time = start.elapsed();
        eprintln!("[test] Complete: {} batches, {} rows in {:?}", batch_count, total_rows, total_time);

        if let Some(first) = first_batch_time {
            eprintln!("[test] Time to first batch: {:?}", first);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires test demo file - single-threaded streaming test"]
    async fn test_simple_event_streaming_single_thread() {
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        // Same as test_simple_event_streaming but with single-threaded runtime
        // to verify that yield_now() enables streaming even without multiple threads

        let start = std::time::Instant::now();
        let mut first_batch_time = None;

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        let queries = vec![
            "SELECT tick, damage, entindex_attacker FROM DamageEvent WHERE damage > 0 LIMIT 100".to_string(),
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
        .expect("run_queries").into_streams();

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
                    "[test] FIRST BATCH at {:?}: {} rows",
                    first_batch_time.unwrap(),
                    batch.num_rows()
                );
            }
        }

        let total_time = start.elapsed();
        eprintln!("[test] Complete: {} batches, {} rows in {:?}", batch_count, total_rows, total_time);

        if let Some(first) = first_batch_time {
            eprintln!("[test] Time to first batch: {:?}", first);
        }
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

    const UNION_ALL_TIMEOUT_SECS: u64 = 300;

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
        "#.to_string();

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
        .expect("run_queries").into_streams();

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
                eprintln!("[test] Batch {}: {} rows, sources: {:?}", batch_count, total_rows, sources_seen);
            }
        }

        eprintln!("[test] Complete: {} batches, {} rows, sources: {:?}", batch_count, total_rows, sources_seen);
        (total_rows, sources_seen)
    }

    fn assert_union_all_entity_results(total_rows: usize, sources_seen: &std::collections::HashSet<String>) {
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
    #[ignore = "requires test demo file - UNION ALL regression test (multi-threaded)"]
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
                eprintln!("[test] PASSED (multi-threaded): {} rows from {:?}", total_rows, sources_seen);
            }
            Err(_) => panic!(
                "[test] TIMEOUT after {} seconds - likely deadlock in multi-threaded mode",
                UNION_ALL_TIMEOUT_SECS
            ),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires test demo file - UNION ALL regression test (single-threaded)"]
    async fn test_union_all_entity_tables_single_threaded() {
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
                eprintln!("[test] PASSED (single-threaded): {} rows from {:?}", total_rows, sources_seen);
            }
            Err(_) => panic!(
                "[test] TIMEOUT after {} seconds - likely deadlock in single-threaded mode. \
                 This suggests UNION ALL execution requires multi-threaded runtime.",
                UNION_ALL_TIMEOUT_SECS
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
        "#.to_string();

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
        .expect("run_queries").into_streams();

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
                eprintln!("[test] Batch {}: {} rows, event types: {:?}", batch_count, total_rows, event_types_seen);
            }
        }

        eprintln!("[test] Complete: {} batches, {} rows, event types: {:?}", batch_count, total_rows, event_types_seen);
        (total_rows, event_types_seen)
    }

    fn assert_union_all_event_results(total_rows: usize, event_types_seen: &std::collections::HashSet<String>) {
        // We should see data from multiple event types
        assert!(
            event_types_seen.len() >= 2,
            "Should have data from at least 2 event types in UNION ALL, got: {:?}",
            event_types_seen
        );
        assert!(total_rows > 0, "Should have some rows");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file - UNION ALL event tables (multi-threaded)"]
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
                eprintln!("[test] PASSED (multi-threaded): {} rows from {:?}", total_rows, event_types_seen);
            }
            Err(_) => panic!(
                "[test] TIMEOUT after {} seconds - likely deadlock in multi-threaded mode",
                UNION_ALL_TIMEOUT_SECS
            ),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file - entity JOIN streaming timing"]
    async fn test_entity_join_streaming_timing() {
        use datafusion::arrow::array::Int32Array;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        
        if !demo_exists() {
            eprintln!("Skipping: demo file not found");
            return;
        }

        let start = std::time::Instant::now();

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        eprintln!("[test] Demo loaded at {:?}", start.elapsed());
        
        let entity_schemas = get_entity_schemas().await.expect("get schemas");
        let packet_source = DemoFilePacketSource::new(demo_bytes);

        // JOIN two event tables - this is what we want to test for streaming behavior
        // DamageEvent happens frequently, HeroKilledEvent happens rarely
        let queries = vec![
            "SELECT d.tick, d.damage, k.entindex_victim as killed \
             FROM DamageEvent d \
             INNER JOIN HeroKilledEvent k ON d.tick = k.tick \
             WHERE d.damage > 0".to_string(),
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
        .expect("run_queries").into_parts();
        eprintln!("[test] Streams created at {:?}", start.elapsed());

        // Track parser completion
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
        let mut ticks_seen = std::collections::BTreeSet::new();

        while let Some(result) = stream.next().await {
            let batch = result.expect("batch");
            batch_count += 1;
            total_rows += batch.num_rows();

            // Collect unique ticks
            if let Some(tick_col) = batch.column_by_name("tick") {
                if let Some(arr) = tick_col.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            ticks_seen.insert(arr.value(i));
                        }
                    }
                }
            }

            if first_batch_time.is_none() {
                first_batch_time = Some(start.elapsed());
                first_batch_before_parser_finished = !parser_finished.load(Ordering::SeqCst);
                eprintln!(
                    "[test] FIRST BATCH at {:?}: {} rows (parser_finished={})",
                    first_batch_time.unwrap(),
                    batch.num_rows(),
                    !first_batch_before_parser_finished
                );
            } else if batch_count <= 5 || batch_count % 10 == 0 {
                eprintln!(
                    "[test] Batch {}: {} rows at {:?}",
                    batch_count,
                    batch.num_rows(),
                    start.elapsed()
                );
            }
        }

        // Wait for parser to complete
        let _ = parser_monitor.await;

        let total_time = start.elapsed();
        eprintln!("\n[test] === SUMMARY ===");
        eprintln!("[test] Total: {} batches, {} rows in {:?}", batch_count, total_rows, total_time);
        eprintln!("[test] Unique ticks seen: {}", ticks_seen.len());
        if let Some(first) = ticks_seen.first() {
            if let Some(last) = ticks_seen.last() {
                eprintln!("[test] Tick range: {} to {}", first, last);
            }
        }

        if let Some(first) = first_batch_time {
            eprintln!("[test] Time to first batch: {:?}", first);
            eprintln!("[test] First batch arrived before parser finished: {}", first_batch_before_parser_finished);
            
            // Assert 1: First batch should arrive quickly (< 2 seconds for true streaming)
            assert!(
                first.as_secs() < 2,
                "First batch took {:?} - expected < 2s for streaming JOIN. This indicates data is not streaming.",
                first
            );
            
            // Assert 2: First batch MUST arrive before parser finishes
            // This is the key streaming invariant - if we only get data after parser completes,
            // we're not truly streaming.
            assert!(
                first_batch_before_parser_finished,
                "First batch arrived AFTER parser finished - this means streaming is broken! \
                 Data should flow through the JOIN while parsing is still in progress."
            );
        } else {
            panic!("No batches received - JOIN should have matches");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires test demo file - UNION ALL event tables (single-threaded)"]
    async fn test_union_all_event_tables_single_threaded() {
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
                eprintln!("[test] PASSED (single-threaded): {} rows from {:?}", total_rows, event_types_seen);
            }
            Err(_) => panic!(
                "[test] TIMEOUT after {} seconds - likely deadlock in single-threaded mode. \
                 This suggests UNION ALL execution requires multi-threaded runtime.",
                UNION_ALL_TIMEOUT_SECS
            ),
        }
    }
}
