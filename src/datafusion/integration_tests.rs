//! Integration tests for SQL queries over demo files.
//!
//! These tests validate the full pipeline from schema discovery through query execution
//! using the unified `DemoSource` API.
//!
//! # Running tests
//!
//! Set `TEST_DEMO_PATH` to a demo file:
//! ```bash
//! TEST_DEMO_PATH=/path/to/demo.dem cargo test --lib -- --ignored
//! ```

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::prelude::*;
    use futures::StreamExt;

    use crate::datafusion::pipeline_analysis::analyze_pipeline;
    use crate::datafusion::table_providers::{EntityTableProvider, EventTableProvider};
    use crate::demo::DemoSource;
    use crate::events::{EventType, event_schema};
    use crate::session::IntoStreamingSession;

    fn test_demo_path() -> String {
        std::env::var("TEST_DEMO_PATH").unwrap_or_else(|_| "test.dem".to_string())
    }

    async fn load_demo_bytes() -> Bytes {
        Bytes::from(
            tokio::fs::read(test_demo_path())
                .await
                .expect("read demo file"),
        )
    }

    // =========================================================================
    // Schema Tests (no demo file required)
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
        let damage_field_count = damage_schema.fields().len();
        assert!(
            damage_field_count >= 5,
            "DamageEvent should have at least tick + 4 data fields (victim, attacker, damage, health), got {}",
            damage_field_count
        );

        let hero_killed_schema = event_schema("HeroKilledEvent").expect("HeroKilledEvent schema");
        let kill_field_count = hero_killed_schema.fields().len();
        assert!(
            kill_field_count >= 3,
            "HeroKilledEvent should have at least tick + victim + attacker fields, got {}",
            kill_field_count
        );
    }

    #[test]
    fn test_damage_event_schema_fields() {
        let schema = event_schema("DamageEvent").expect("DamageEvent schema");

        let required_fields = [
            ("tick", "tracking when the event occurred"),
            ("damage", "the damage amount"),
            ("entindex_victim", "who took damage"),
            ("entindex_attacker", "who dealt damage"),
        ];

        for (field_name, purpose) in required_fields {
            assert!(
                schema.field_with_name(field_name).is_ok(),
                "DamageEvent missing '{}' field needed for {}",
                field_name,
                purpose
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
            "EventType::all() should return all variants (Deadlock has 50+ event types), got {}",
            all_events.len()
        );
    }

    // =========================================================================
    // Query Plan Tests (no demo file required)
    // =========================================================================

    fn streaming_session_config() -> SessionConfig {
        SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false)
    }

    #[tokio::test]
    async fn test_join_event_tables_plan_selection() {
        let ctx = SessionContext::new_with_config(streaming_session_config());

        let damage_schema = event_schema("DamageEvent").expect("damage schema");
        let damage_provider = EventTableProvider::new(EventType::Damage, damage_schema);

        let kill_schema = event_schema("HeroKilledEvent").expect("kill schema");
        let kill_provider = EventTableProvider::new(EventType::HeroKilled, kill_schema);

        ctx.register_table("DamageEvent", Arc::new(damage_provider))
            .unwrap();
        ctx.register_table("HeroKilledEvent", Arc::new(kill_provider))
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

        let plan_str = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();

        assert!(
            plan_str.contains("SymmetricHashJoinExec"),
            "Expected streaming SymmetricHashJoinExec for unbounded sources, got:\n{}",
            plan_str
        );

        assert!(
            !plan_str.contains("RepartitionExec"),
            "target_partitions=1 should prevent repartitioning, got:\n{}",
            plan_str
        );
    }

    #[tokio::test]
    async fn test_join_pipeline_properties() {
        let ctx = SessionContext::new_with_config(streaming_session_config());

        let damage_schema = event_schema("DamageEvent").expect("damage schema");
        let damage_provider = EventTableProvider::new(EventType::Damage, damage_schema);

        let kill_schema = event_schema("HeroKilledEvent").expect("kill schema");
        let kill_provider = EventTableProvider::new(EventType::HeroKilled, kill_schema);

        ctx.register_table("DamageEvent", Arc::new(damage_provider))
            .unwrap();
        ctx.register_table("HeroKilledEvent", Arc::new(kill_provider))
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

        assert!(
            analysis.is_streaming_safe(),
            "JOIN on tick should be streaming-safe (no pipeline breakers):\n{}",
            analysis.report()
        );
    }

    #[tokio::test]
    async fn test_join_event_entity_plan_selection() {
        let ctx = SessionContext::new_with_config(streaming_session_config());

        let damage_schema = event_schema("DamageEvent").expect("damage schema");
        let damage_provider = EventTableProvider::new(EventType::Damage, damage_schema);
        ctx.register_table("DamageEvent", Arc::new(damage_provider))
            .unwrap();

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
        ]);
        let entity_provider =
            EntityTableProvider::new(Arc::new(entity_schema), Arc::from("CCitadelPlayerPawn"));
        ctx.register_table("CCitadelPlayerPawn", Arc::new(entity_provider))
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

        assert!(
            plan_str.contains("SymmetricHashJoinExec"),
            "Expected streaming SymmetricHashJoinExec for event-entity JOIN, got:\n{}",
            plan_str
        );

        assert!(
            !plan_str.contains("RepartitionExec"),
            "target_partitions=1 should prevent repartitioning, got:\n{}",
            plan_str
        );
    }

    #[tokio::test]
    async fn test_union_all_plan_structure() {
        let ctx = SessionContext::new_with_config(streaming_session_config());

        let damage_schema = event_schema("DamageEvent").expect("damage schema");
        let damage_provider = EventTableProvider::new(EventType::Damage, damage_schema);

        let kill_schema = event_schema("HeroKilledEvent").expect("kill schema");
        let kill_provider = EventTableProvider::new(EventType::HeroKilled, kill_schema);

        ctx.register_table("DamageEvent", Arc::new(damage_provider))
            .unwrap();
        ctx.register_table("HeroKilledEvent", Arc::new(kill_provider))
            .unwrap();

        let sql = r#"
            SELECT tick, 'Damage' as event_type FROM DamageEvent WHERE tick < 10000
            UNION ALL
            SELECT tick, 'HeroKilled' as event_type FROM HeroKilledEvent WHERE tick < 10000
        "#;

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

        // Print the plan for diagnostic visibility
        eprintln!("UNION ALL physical plan:\n{}", plan_str);

        // UNION ALL should use InterleaveExec or UnionExec
        assert!(
            plan_str.contains("InterleaveExec") || plan_str.contains("UnionExec"),
            "Expected InterleaveExec or UnionExec for UNION ALL, got:\n{}",
            plan_str
        );

        // Should have two StreamingTableExec leaves (one per table)
        let streaming_count = plan_str.matches("StreamingTableExec").count();
        eprintln!("StreamingTableExec count: {}", streaming_count);

        // Check pipeline safety
        let analysis = analyze_pipeline(&physical);
        eprintln!("Pipeline analysis:\n{}", analysis.report());
        eprintln!("Is streaming safe: {}", analysis.is_streaming_safe());

        // Check partition count — this is critical for understanding the bug
        let output_partitioning = physical.output_partitioning();
        eprintln!(
            "Output partitioning: {:?}, partition_count: {}",
            output_partitioning,
            output_partitioning.partition_count()
        );
    }

    #[tokio::test]
    async fn test_union_all_entity_plan_structure() {
        let ctx = SessionContext::new_with_config(streaming_session_config());

        let pawn_schema = datafusion::arrow::datatypes::Schema::new(vec![
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
        ]);
        let pawn_provider =
            EntityTableProvider::new(Arc::new(pawn_schema), Arc::from("CCitadelPlayerPawn"));

        let controller_schema = datafusion::arrow::datatypes::Schema::new(vec![
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
        ]);
        let controller_provider = EntityTableProvider::new(
            Arc::new(controller_schema),
            Arc::from("CCitadelPlayerController"),
        );

        ctx.register_table("CCitadelPlayerPawn", Arc::new(pawn_provider))
            .unwrap();
        ctx.register_table("CCitadelPlayerController", Arc::new(controller_provider))
            .unwrap();

        let sql = r#"
            SELECT tick, entity_index, 'Pawn' as source_type
            FROM CCitadelPlayerPawn
            WHERE tick < 1000
            UNION ALL
            SELECT tick, entity_index, 'Controller' as source_type
            FROM CCitadelPlayerController
            WHERE tick < 1000
        "#;

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

        eprintln!("UNION ALL entity plan:\n{}", plan_str);

        let analysis = analyze_pipeline(&physical);
        eprintln!("Pipeline analysis:\n{}", analysis.report());
        eprintln!("Is streaming safe: {}", analysis.is_streaming_safe());

        let output_partitioning = physical.output_partitioning();
        eprintln!(
            "Output partitioning: {:?}, partition_count: {}",
            output_partitioning,
            output_partitioning.partition_count()
        );
    }

    // =========================================================================
    // Query Execution Tests (require demo file)
    // =========================================================================

    async fn run_query(sql: &str) -> Vec<RecordBatch> {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let mut handle = session.add_query(sql).await.expect("add_query");
        let _result = session.start().expect("start");

        let mut batches = Vec::new();
        while let Some(result) = handle.next().await {
            batches.push(result.expect("batch"));
        }
        batches
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn extract_i32_column(batches: &[RecordBatch], column: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                let col = batch
                    .column_by_name(column)
                    .unwrap_or_else(|| panic!("column '{}' not found", column));
                let arr = col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap_or_else(|| panic!("column '{}' is not Int32", column));
                arr.iter().map(|v| v.unwrap_or(0)).collect::<Vec<_>>()
            })
            .collect()
    }

    fn assert_monotonic_ticks(batches: &[RecordBatch]) {
        let ticks = extract_i32_column(batches, "tick");
        for window in ticks.windows(2) {
            assert!(
                window[0] <= window[1],
                "ticks should be monotonically increasing within batches, found {} > {}",
                window[0],
                window[1]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_event_query() {
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_query("SELECT tick, damage FROM DamageEvent LIMIT 100"),
        )
        .await
        .expect("query should complete within 30s");

        assert!(!batches.is_empty(), "should return at least one batch");
        assert!(total_rows(&batches) > 0, "should have damage events");

        let first_batch = &batches[0];
        assert!(
            first_batch.schema().field_with_name("tick").is_ok(),
            "result should contain tick column"
        );
        assert!(
            first_batch.schema().field_with_name("damage").is_ok(),
            "result should contain damage column"
        );

        assert_monotonic_ticks(&batches);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_entity_query() {
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_query("SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 100"),
        )
        .await
        .expect("query should complete within 30s");

        assert!(!batches.is_empty(), "should return at least one batch");
        assert!(total_rows(&batches) > 0, "should have entity data");

        let first_batch = &batches[0];
        assert!(
            first_batch.schema().field_with_name("tick").is_ok(),
            "result should contain tick column"
        );
        assert!(
            first_batch.schema().field_with_name("entity_index").is_ok(),
            "result should contain entity_index column"
        );

        assert_monotonic_ticks(&batches);

        let entity_indices = extract_i32_column(&batches, "entity_index");
        assert!(
            entity_indices.iter().all(|&idx| idx >= 0),
            "entity_index should be non-negative"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_nested_fields_query() {
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_query("SELECT tick, damage, flags FROM DamageEvent LIMIT 100"),
        )
        .await
        .expect("query should complete within 30s");

        assert!(!batches.is_empty(), "should return at least one batch");

        let first_batch = &batches[0];
        assert_eq!(
            first_batch.num_columns(),
            3,
            "should have exactly 3 columns (tick, damage, flags)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_event_event_join() {
        let sql = "SELECT d.tick, d.damage, k.entindex_victim \
                   FROM DamageEvent d \
                   INNER JOIN HeroKilledEvent k ON d.tick = k.tick \
                   LIMIT 50";

        let batches = tokio::time::timeout(std::time::Duration::from_secs(60), run_query(sql))
            .await
            .expect("JOIN query should complete within 60s");

        let first_batch = &batches[0];
        assert_eq!(first_batch.num_columns(), 3, "JOIN should return 3 columns");

        assert_monotonic_ticks(&batches);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_event_entity_join() {
        let sql = "SELECT d.tick, d.damage, p.entity_index \
                   FROM DamageEvent d \
                   INNER JOIN CCitadelPlayerPawn p ON d.tick = p.tick \
                   LIMIT 50";

        let batches = tokio::time::timeout(std::time::Duration::from_secs(60), run_query(sql))
            .await
            .expect("JOIN query should complete within 60s");

        assert!(!batches.is_empty(), "JOIN should produce results");

        let first_batch = &batches[0];
        assert_eq!(first_batch.num_columns(), 3, "JOIN should return 3 columns");

        assert_monotonic_ticks(&batches);
    }

    // =========================================================================
    // Multi-Query Tests (require demo file)
    //
    // These test that multiple queries registered via add_query() all receive
    // correct data when drained concurrently. This exercises the
    // drain_pending_slots -> distribution channel -> ReceiverSlot wiring.
    // =========================================================================

    /// Two queries on different entity tables, drained concurrently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_multi_query_different_entity_tables() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let mut h1 = session
            .add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 100")
            .await
            .expect("add_query 1");
        let mut h2 = session
            .add_query("SELECT tick, entity_index FROM CCitadelPlayerController LIMIT 50")
            .await
            .expect("add_query 2");

        let _result = session.start().expect("start");

        let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let f1 = async {
                let mut batches = Vec::new();
                while let Some(result) = h1.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            let f2 = async {
                let mut batches = Vec::new();
                while let Some(result) = h2.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            tokio::join!(f1, f2)
        })
        .await
        .expect("multi-query should complete within 30s");

        assert_eq!(total_rows(&r1), 100, "pawn query should return 100 rows");
        assert_eq!(
            total_rows(&r2),
            50,
            "controller query should return 50 rows"
        );
        assert_monotonic_ticks(&r1);
        assert_monotonic_ticks(&r2);
    }

    /// Two queries on the same entity table (different filters).
    /// This creates two ReceiverSlots on the same provider — the provider's
    /// data must be broadcast to both.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_multi_query_same_entity_table() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let mut h1 = session
            .add_query(
                "SELECT tick, entity_index FROM CCitadelPlayerPawn \
                 WHERE entity_index = 65 LIMIT 20",
            )
            .await
            .expect("add_query 1");
        let mut h2 = session
            .add_query(
                "SELECT tick, entity_index FROM CCitadelPlayerPawn \
                 WHERE entity_index = 72 LIMIT 20",
            )
            .await
            .expect("add_query 2");

        let _result = session.start().expect("start");

        let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let f1 = async {
                let mut batches = Vec::new();
                while let Some(result) = h1.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            let f2 = async {
                let mut batches = Vec::new();
                while let Some(result) = h2.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            tokio::join!(f1, f2)
        })
        .await
        .expect("multi-query same table should complete within 30s");

        assert_eq!(
            total_rows(&r1),
            20,
            "filter entity_index=65 should return 20 rows"
        );
        assert_eq!(
            total_rows(&r2),
            20,
            "filter entity_index=72 should return 20 rows"
        );

        let indices_1 = extract_i32_column(&r1, "entity_index");
        let indices_2 = extract_i32_column(&r2, "entity_index");
        assert!(
            indices_1.iter().all(|&i| i == 65),
            "query 1 should only contain entity_index=65"
        );
        assert!(
            indices_2.iter().all(|&i| i == 72),
            "query 2 should only contain entity_index=72"
        );
    }

    /// Two queries on different event tables, drained concurrently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_multi_query_different_event_tables() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let mut h1 = session
            .add_query("SELECT tick, damage FROM DamageEvent LIMIT 100")
            .await
            .expect("add_query 1");
        let mut h2 = session
            .add_query("SELECT tick, entindex_victim FROM HeroKilledEvent LIMIT 20")
            .await
            .expect("add_query 2");

        let _result = session.start().expect("start");

        let (r1, r2) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            let f1 = async {
                let mut batches = Vec::new();
                while let Some(result) = h1.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            let f2 = async {
                let mut batches = Vec::new();
                while let Some(result) = h2.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            tokio::join!(f1, f2)
        })
        .await
        .expect("multi-query events should complete within 60s");

        assert_eq!(total_rows(&r1), 100, "damage query should return 100 rows");
        assert_eq!(
            total_rows(&r2),
            20,
            "hero killed query should return 20 rows (got {})",
            total_rows(&r2),
        );
        assert_monotonic_ticks(&r1);
        assert_monotonic_ticks(&r2);
    }

    /// Mixed: one entity query + one event query, drained concurrently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_multi_query_entity_and_event() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let mut h_entity = session
            .add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 100")
            .await
            .expect("add entity query");
        let mut h_event = session
            .add_query("SELECT tick, damage FROM DamageEvent LIMIT 50")
            .await
            .expect("add event query");

        let _result = session.start().expect("start");

        let (r_entity, r_event) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let f1 = async {
                let mut batches = Vec::new();
                while let Some(result) = h_entity.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            let f2 = async {
                let mut batches = Vec::new();
                while let Some(result) = h_event.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            tokio::join!(f1, f2)
        })
        .await
        .expect("mixed multi-query should complete within 30s");

        assert_eq!(
            total_rows(&r_entity),
            100,
            "entity query should return 100 rows"
        );
        assert_eq!(
            total_rows(&r_event),
            50,
            "event query should return 50 rows"
        );

        // Verify correct schemas came back
        assert!(
            r_entity[0].schema().field_with_name("entity_index").is_ok(),
            "entity result should have entity_index"
        );
        assert!(
            r_event[0].schema().field_with_name("damage").is_ok(),
            "event result should have damage"
        );
    }

    /// Three queries: two on the same entity table + one event table.
    /// Exercises broadcast (same table, multiple slots) and mixed types simultaneously.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_multi_query_three_queries_mixed() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let mut h1 = session
            .add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 50")
            .await
            .expect("add_query 1");
        let mut h2 = session
            .add_query(
                "SELECT tick, entity_index FROM CCitadelPlayerPawn \
                 WHERE entity_index = 65 LIMIT 10",
            )
            .await
            .expect("add_query 2");
        let mut h3 = session
            .add_query("SELECT tick, damage FROM DamageEvent LIMIT 30")
            .await
            .expect("add_query 3");

        let _result = session.start().expect("start");

        let (r1, r2, r3) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let f1 = async {
                let mut batches = Vec::new();
                while let Some(result) = h1.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            let f2 = async {
                let mut batches = Vec::new();
                while let Some(result) = h2.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            let f3 = async {
                let mut batches = Vec::new();
                while let Some(result) = h3.next().await {
                    batches.push(result.expect("batch"));
                }
                batches
            };
            tokio::join!(f1, f2, f3)
        })
        .await
        .expect("three-query mix should complete within 30s");

        assert_eq!(total_rows(&r1), 50, "pawn query should return 50 rows");
        assert_eq!(
            total_rows(&r2),
            10,
            "filtered pawn query should return 10 rows"
        );
        assert_eq!(
            total_rows(&r3),
            30,
            "damage event query should return 30 rows"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_aggregation_rejected() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let result = session.add_query("SELECT COUNT(*) FROM DamageEvent").await;

        assert!(
            result.is_err(),
            "Aggregation without GROUP BY requires unbounded buffering and should be rejected"
        );
    }

    // =========================================================================
    // UNION ALL Tests (require demo file)
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_union_all_entity_tables() {
        // Use subqueries with LIMIT so each branch terminates early in debug mode,
        // while still exercising the UNION ALL multi-partition plan.
        let sql = r#"
            (SELECT tick, entity_index, 'Pawn' as source_type
             FROM CCitadelPlayerPawn
             LIMIT 50)
            UNION ALL
            (SELECT tick, entity_index, 'Controller' as source_type  
             FROM CCitadelPlayerController
             LIMIT 50)
        "#;

        let batches = tokio::time::timeout(std::time::Duration::from_secs(60), run_query(sql))
            .await
            .expect("UNION ALL should complete within 60s");

        let total = total_rows(&batches);
        assert!(
            total > 50,
            "UNION ALL should return rows from both tables, got {}",
            total
        );
    }

    /// A single plan that scans one table twice, with one consumer driving both.
    ///
    /// Distinct from test_multi_query_same_entity_table, where two independent
    /// queries each drain their own slot: here the join operator interleaves
    /// demand across two slots on the same provider, so a gate that assumes
    /// independent drainage could stall.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_self_join_same_entity_table() {
        // No LIMIT subqueries: those make both sides look bounded, so the planner
        // picks a CollectLeft HashJoinExec and SanityCheckPlan rejects it as a
        // pipeline breaker. Joining the unbounded streams directly is what
        // exercises the streaming symmetric join across two slots of one provider.
        let sql = r#"
            SELECT a.tick, a.entity_index AS a_ent, b.entity_index AS b_ent
            FROM CCitadelPlayerPawn a
            JOIN CCitadelPlayerPawn b ON a.tick = b.tick
            WHERE a.entity_index = 65 AND b.entity_index = 72
            LIMIT 20
        "#;

        let batches = tokio::time::timeout(std::time::Duration::from_secs(90), run_query(sql))
            .await
            .expect("self-join must not deadlock");

        // Terminating at all is the assertion that matters.
        let _ = total_rows(&batches);
    }

    /// UNION ALL of one table with itself: two scans of the same provider under a
    /// single plan, without a join operator mediating demand.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_union_all_same_entity_table() {
        let sql = r#"
            (SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 50)
            UNION ALL
            (SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 50)
        "#;

        let batches = tokio::time::timeout(std::time::Duration::from_secs(60), run_query(sql))
            .await
            .expect("self UNION ALL must not deadlock");

        assert!(total_rows(&batches) > 0, "expected rows from both branches");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_union_all_event_tables() {
        let sql = r#"
            (SELECT tick, 'Damage' as event_type FROM DamageEvent LIMIT 50)
            UNION ALL
            (SELECT tick, 'HeroKilled' as event_type FROM HeroKilledEvent LIMIT 50)
        "#;

        let batches = tokio::time::timeout(std::time::Duration::from_secs(60), run_query(sql))
            .await
            .expect("UNION ALL should complete within 60s");

        let total = total_rows(&batches);
        assert!(
            total > 50,
            "UNION ALL should return rows from both event tables, got {}",
            total
        );
    }

    // =========================================================================
    // Truncation regression tests
    //
    // The bug these guard: on 106452596.dem a tracked CNPC_TrooperNeutral was
    // CREATEd at entity index 2520 over a stale entry in the entity container's
    // `skipped_serializers` map. `handle_update` reads that map first, so every
    // later UPDATE for the index was decoded against the wrong field layout and
    // the parse died with "field path not found" at tick 21,131 of 126,162.
    //
    // The parser task swallowed that error (`let _ = parser.run_to_end()`), so
    // every distribution channel simply closed and every query stream ended
    // *cleanly* on partial data. The pass reported success while dropping
    // 83% of the match.
    //
    // Two properties have to hold, and they fail independently:
    //   1. A table's row count must not depend on what else was registered.
    //   2. If the parse does die, every live stream must say so.
    // =========================================================================

    /// Drain a handle to exhaustion, keeping whichever comes first: all the
    /// batches, or the error that ended it.
    async fn drain(mut handle: crate::session::QueryHandle) -> (Vec<RecordBatch>, Option<String>) {
        let mut batches = Vec::new();
        while let Some(result) = handle.next().await {
            match result {
                Ok(batch) => batches.push(batch),
                Err(e) => return (batches, Some(e.to_string())),
            }
        }
        (batches, None)
    }

    fn last_tick(batches: &[RecordBatch]) -> Option<i32> {
        batches.iter().rev().find_map(|b| {
            let col = b.column_by_name("tick")?;
            let arr = col.as_any().downcast_ref::<Int32Array>()?;
            (!arr.is_empty()).then(|| arr.value(arr.len() - 1))
        })
    }

    /// A table registered alongside another must yield exactly what it yields
    /// alone.
    ///
    /// The measured table is the *innocent* one and the sibling is the one that
    /// used to kill the parse: pre-fix, CNPC_TrooperNeutral's decode aborted the
    /// shared parse at tick 21,131 and every other registered table's stream was
    /// cut off at the same tick. Row counts, not just "some rows", because
    /// truncation is invisible in the shape of the output — only in how much of
    /// it there is.
    ///
    /// On a demo that never trips the bug this is simply a tautology that
    /// passes, which is the right behaviour for a test that has to run against
    /// whatever `TEST_DEMO_PATH` points at.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_sibling_query_does_not_truncate_a_stream() {
        let measured = "SELECT tick, entity_index, delta_type FROM CCitadelPlayerController";
        let sibling = "SELECT tick, entity_index, delta_type FROM CNPC_TrooperNeutral";

        // Pass 1: the measured table alone.
        let alone = {
            let source = DemoSource::from_bytes(load_demo_bytes().await);
            let mut session = source.into_session().await.expect("into_session");
            let handle = session.add_query(measured).await.expect("add_query alone");
            let _result = session.start().expect("start");

            tokio::time::timeout(std::time::Duration::from_secs(300), drain(handle))
                .await
                .expect("solo query should finish within 300s")
        };
        assert_eq!(alone.1, None, "solo query errored");
        assert!(
            total_rows(&alone.0) > 0,
            "solo query produced no rows; the test tables are wrong for this demo"
        );

        // Pass 2: the same table with the sibling registered beside it.
        let together = {
            let source = DemoSource::from_bytes(load_demo_bytes().await);
            let mut session = source.into_session().await.expect("into_session");
            let handle_a = session.add_query(measured).await.expect("add_query a");
            let handle_b = session.add_query(sibling).await.expect("add_query b");
            let _result = session.start().expect("start");

            let (a, _b) = tokio::time::timeout(std::time::Duration::from_secs(300), async {
                tokio::join!(drain(handle_a), drain(handle_b))
            })
            .await
            .expect("paired queries should finish within 300s");
            a
        };
        assert_eq!(together.1, None, "paired query errored");

        assert_eq!(
            total_rows(&together.0),
            total_rows(&alone.0),
            "registering a second table changed the first table's row count \
             ({} alone vs {} together) — a sibling stream truncated this one",
            total_rows(&alone.0),
            total_rows(&together.0),
        );
        assert_eq!(
            last_tick(&together.0),
            last_tick(&alone.0),
            "registering a second table changed the tick the first table reached"
        );
    }

    /// If the parse dies, every live stream must say so.
    ///
    /// The parser task's result and the query streams' results have to agree: a
    /// parser that reports failure must not leave behind handles that ended as
    /// if the demo simply ran out. A green assertion here is either "the parse
    /// succeeded and no stream errored" or "it failed and every stream reported
    /// it" — never "it failed and the streams were quiet about it", which is
    /// what `failures() == {}` on a truncated pass used to mean.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_parse_failure_is_never_a_silent_truncation() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let h1 = session
            .add_query("SELECT tick, entity_index, delta_type FROM CCitadelPlayerController")
            .await
            .expect("add_query 1");
        let h2 = session
            .add_query("SELECT tick, entity_index, delta_type FROM CNPC_TrooperNeutral")
            .await
            .expect("add_query 2");
        let h3 = session
            .add_query("SELECT tick FROM DamageEvent")
            .await
            .expect("add_query 3");

        let result = session.start().expect("start");

        let drained = tokio::time::timeout(std::time::Duration::from_secs(300), async {
            tokio::join!(drain(h1), drain(h2), drain(h3))
        })
        .await
        .expect("queries should finish within 300s");

        let parser_outcome = result.parser_handle.await.expect("parser task panicked");
        let errors = [&drained.0.1, &drained.1.1, &drained.2.1];

        // First: the parser's result and the streams' results must agree. A
        // parse that died must not leave behind handles that ended as if the
        // demo simply ran out.
        match &parser_outcome {
            Ok(()) => {
                for (i, err) in errors.iter().enumerate() {
                    assert!(
                        err.is_none(),
                        "stream {i} failed although the parser reported success: {err:?}"
                    );
                }
            }
            Err(e) => {
                for (i, err) in errors.iter().enumerate() {
                    assert!(
                        err.is_some(),
                        "the parse failed ({e}) but stream {i} ended cleanly — \
                         this is the silent truncation the fault channel exists to prevent",
                    );
                }
            }
        }

        // Second: on a demo we can parse at all, the parse must actually finish.
        // Before the entity-index fix this demo aborted with "field path not
        // found" at tick 21,131 and every registered stream was cut short. That
        // is now loud rather than silent — and a loud failure here is a real
        // one, not a flaky test.
        assert!(
            parser_outcome.is_ok(),
            "the parse did not run to the end of the demo: {:?}",
            parser_outcome.unwrap_err()
        );
    }

    /// Every query hitting its LIMIT is a legitimate reason to stop parsing
    /// early, and must not be reported as a fault.
    ///
    /// Guards the other side of the fix: the parser stops with
    /// `ArrowVisitorError::ChannelClosed` once no consumer is left, and that
    /// path must stay silent. A regression here turns every satisfied LIMIT
    /// query into a spurious failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file"]
    async fn test_all_limits_satisfied_is_not_a_failure() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let mut session = source.into_session().await.expect("into_session");

        let h1 = session
            .add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 10")
            .await
            .expect("add_query 1");
        let h2 = session
            .add_query("SELECT tick, entity_index FROM CCitadelPlayerController LIMIT 10")
            .await
            .expect("add_query 2");

        let result = session.start().expect("start");

        let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            tokio::join!(drain(h1), drain(h2))
        })
        .await
        .expect("limited queries should finish within 60s");

        assert_eq!(a.1, None, "limited query 1 must not report an error");
        assert_eq!(b.1, None, "limited query 2 must not report an error");
        assert_eq!(total_rows(&a.0), 10);
        assert_eq!(total_rows(&b.0), 10);

        assert!(
            result
                .parser_handle
                .await
                .expect("parser task panicked")
                .is_ok(),
            "stopping because every consumer is satisfied is not a parse failure"
        );
    }
}
