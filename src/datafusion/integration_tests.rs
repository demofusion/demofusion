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
        let damage_provider =
            EventTableProvider::new(EventType::Damage, damage_schema);

        let kill_schema = event_schema("HeroKilledEvent").expect("kill schema");
        let kill_provider =
            EventTableProvider::new(EventType::HeroKilled, kill_schema);

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
        let damage_provider =
            EventTableProvider::new(EventType::Damage, damage_schema);

        let kill_schema = event_schema("HeroKilledEvent").expect("kill schema");
        let kill_provider =
            EventTableProvider::new(EventType::HeroKilled, kill_schema);

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
        let damage_provider =
            EventTableProvider::new(EventType::Damage, damage_schema);
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
        let entity_provider = EntityTableProvider::new(
            Arc::new(entity_schema),
            Arc::from("CCitadelPlayerPawn"),
        );
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

    // =========================================================================
    // Query Execution Tests (require demo file)
    // =========================================================================

    async fn run_query(sql: &str) -> Vec<RecordBatch> {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let (mut session, _schemas) = source.into_session().await.expect("into_session");

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_aggregation_rejected() {
        let source = DemoSource::from_bytes(load_demo_bytes().await);
        let (mut session, _schemas) = source.into_session().await.expect("into_session");

        let result = session.add_query("SELECT COUNT(*) FROM DamageEvent").await;

        assert!(
            result.is_err(),
            "Aggregation without GROUP BY requires unbounded buffering and should be rejected"
        );
    }

    // =========================================================================
    // Known Failing Tests (UNION ALL bug - keep for regression tracking)
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file - known failing: UNION ALL multi-partition bug"]
    async fn test_union_all_entity_tables() {
        let sql = r#"
            SELECT tick, entity_index, 'Pawn' as source_type
            FROM CCitadelPlayerPawn
            WHERE tick < 1000
            UNION ALL
            SELECT tick, entity_index, 'Controller' as source_type  
            FROM CCitadelPlayerController
            WHERE tick < 1000
        "#;

        let batches = tokio::time::timeout(std::time::Duration::from_secs(60), run_query(sql))
            .await
            .expect("UNION ALL should complete within 60s");

        assert!(
            total_rows(&batches) > 0,
            "UNION ALL should return rows from both tables"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file - known failing: UNION ALL multi-partition bug"]
    async fn test_union_all_event_tables() {
        let sql = r#"
            SELECT tick, 'Damage' as event_type FROM DamageEvent WHERE tick < 10000
            UNION ALL
            SELECT tick, 'BulletHit' as event_type FROM BulletHitEvent WHERE tick < 10000
            UNION ALL
            SELECT tick, 'HeroKilled' as event_type FROM HeroKilledEvent WHERE tick < 10000
        "#;

        let batches = tokio::time::timeout(std::time::Duration::from_secs(60), run_query(sql))
            .await
            .expect("UNION ALL should complete within 60s");

        assert!(
            total_rows(&batches) > 0,
            "UNION ALL should return rows from all event tables"
        );
    }
}
