//! Integration tests for SQL queries over demo files.
//!
//! These tests validate the full pipeline from schema discovery through query execution
//! using the unified `DemoSource` API.
//!
//! # Running tests
//!
//! Set `TEST_DEMO_PATH` to a demo file:
//! ```bash
//! TEST_DEMO_PATH=/path/to/demo.dem cargo test --all-features -- --ignored
//! ```

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bytes::Bytes;
    use datafusion::arrow::record_batch::RecordBatch;
    use futures::StreamExt;

    use crate::demo::DemoSource;
    use crate::events::{EventType, event_schema};
    use crate::session::IntoStreamingSession;

    fn test_demo_path() -> String {
        std::env::var("TEST_DEMO_PATH").unwrap_or_else(|_| "test.dem".to_string())
    }

    fn demo_exists() -> bool {
        Path::new(&test_demo_path()).exists()
    }

    async fn load_demo_bytes() -> Option<Bytes> {
        if !demo_exists() {
            return None;
        }
        tokio::fs::read(test_demo_path())
            .await
            .ok()
            .map(Bytes::from)
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
    // Query Plan Tests (no demo file required)
    // =========================================================================

    #[tokio::test]
    async fn test_join_event_tables_plan_selection() {
        use crate::datafusion::table_providers::{EventTableProvider, new_receiver_slot};
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

        let logical = ctx.state().create_logical_plan(sql).await.expect("logical plan");
        let physical = ctx.state().create_physical_plan(&logical).await.expect("physical plan");

        let plan_str = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();

        assert!(
            plan_str.contains("SymmetricHashJoinExec"),
            "Expected SymmetricHashJoinExec for event table JOIN, got:\n{}",
            plan_str
        );

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

        let logical = ctx.state().create_logical_plan(sql).await.expect("logical plan");
        let physical = ctx.state().create_physical_plan(&logical).await.expect("physical plan");

        let analysis = analyze_pipeline(&physical);

        assert!(
            analysis.is_streaming_safe(),
            "Physical plan has pipeline breakers!\n{}",
            analysis.report()
        );
    }

    #[tokio::test]
    async fn test_join_event_entity_plan_selection() {
        use crate::datafusion::table_providers::{
            EntityTableProvider, EventTableProvider, new_receiver_slot,
        };
        use datafusion::prelude::*;

        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_coalesce_batches(false);
        let ctx = SessionContext::new_with_config(config);

        let damage_schema = event_schema("DamageEvent").expect("damage schema");
        let damage_slot = new_receiver_slot();
        let damage_provider =
            EventTableProvider::new(EventType::Damage, damage_schema, damage_slot);
        ctx.register_table("DamageEvent", std::sync::Arc::new(damage_provider))
            .unwrap();

        let entity_schema = datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("tick", datafusion::arrow::datatypes::DataType::Int32, false),
            datafusion::arrow::datatypes::Field::new("entity_index", datafusion::arrow::datatypes::DataType::Int32, false),
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

        let logical = ctx.state().create_logical_plan(sql).await.expect("logical plan");
        let physical = ctx.state().create_physical_plan(&logical).await.expect("physical plan");

        let plan_str = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();

        assert!(
            plan_str.contains("SymmetricHashJoinExec"),
            "Expected SymmetricHashJoinExec for event-entity JOIN, got:\n{}",
            plan_str
        );

        assert!(
            !plan_str.contains("RepartitionExec"),
            "Should not have RepartitionExec, got:\n{}",
            plan_str
        );
    }

    // =========================================================================
    // Query Execution Tests (require demo file)
    // =========================================================================

    async fn run_single_query(sql: &str) -> Vec<RecordBatch> {
        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let source = DemoSource::from_bytes(demo_bytes);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_event_query() {
        if !demo_exists() {
            return;
        }

        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_single_query("SELECT tick, damage FROM DamageEvent LIMIT 100"),
        )
        .await
        .expect("timeout");

        assert!(total_rows(&batches) > 0, "Should have damage events");

        if let Some(batch) = batches.first() {
            assert!(batch.schema().field_with_name("tick").is_ok());
            assert!(batch.schema().field_with_name("damage").is_ok());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_entity_query() {
        if !demo_exists() {
            return;
        }

        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_single_query("SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 100"),
        )
        .await
        .expect("timeout");

        assert!(total_rows(&batches) > 0, "Should have pawn data");

        if let Some(batch) = batches.first() {
            assert!(batch.schema().field_with_name("tick").is_ok());
            assert!(batch.schema().field_with_name("entity_index").is_ok());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_nested_fields_query() {
        if !demo_exists() {
            return;
        }

        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            run_single_query("SELECT tick, damage, origin FROM DamageEvent LIMIT 100"),
        )
        .await
        .expect("timeout");

        assert!(total_rows(&batches) > 0, "Should have damage events");

        if let Some(batch) = batches.first() {
            assert!(
                batch.schema().field_with_name("origin").is_ok(),
                "Should have nested origin field"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_event_event_join() {
        if !demo_exists() {
            return;
        }

        let sql = "SELECT d.tick, d.damage, k.entindex_victim \
                   FROM DamageEvent d \
                   INNER JOIN HeroKilledEvent k ON d.tick = k.tick \
                   LIMIT 50";

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run_single_query(sql),
        )
        .await;

        match result {
            Ok(batches) => {
                eprintln!("[test] Event-event JOIN returned {} rows", total_rows(&batches));
            }
            Err(_) => panic!("TIMEOUT after 60 seconds"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_event_entity_join() {
        if !demo_exists() {
            return;
        }

        let sql = "SELECT d.tick, d.damage, p.entity_index \
                   FROM DamageEvent d \
                   INNER JOIN CCitadelPlayerPawn p ON d.tick = p.tick \
                   LIMIT 50";

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run_single_query(sql),
        )
        .await;

        match result {
            Ok(batches) => {
                eprintln!("[test] Event-entity JOIN returned {} rows", total_rows(&batches));
            }
            Err(_) => panic!("TIMEOUT after 60 seconds"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires test demo file"]
    async fn test_aggregation_rejected() {
        if !demo_exists() {
            return;
        }

        let demo_bytes = load_demo_bytes().await.expect("load demo");
        let source = DemoSource::from_bytes(demo_bytes);
        let (mut session, _schemas) = source.into_session().await.expect("into_session");

        let result = session.add_query("SELECT COUNT(*) FROM DamageEvent").await;

        assert!(
            result.is_err(),
            "Aggregation on unbounded stream should be rejected"
        );
    }

    // =========================================================================
    // Known Failing Tests (UNION ALL bug - keep for regression tracking)
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file - known failing: UNION ALL multi-partition bug"]
    async fn test_union_all_entity_tables() {
        if !demo_exists() {
            return;
        }

        let sql = r#"
            SELECT tick, entity_index, 'Pawn' as source_type
            FROM CCitadelPlayerPawn
            WHERE tick < 1000
            UNION ALL
            SELECT tick, entity_index, 'Controller' as source_type  
            FROM CCitadelPlayerController
            WHERE tick < 1000
        "#;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run_single_query(sql),
        )
        .await;

        match result {
            Ok(batches) => {
                let rows = total_rows(&batches);
                eprintln!("[test] UNION ALL returned {} rows", rows);
                assert!(rows > 0, "Should have rows from UNION ALL");
            }
            Err(_) => panic!("TIMEOUT after 60 seconds"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires test demo file - known failing: UNION ALL multi-partition bug"]
    async fn test_union_all_event_tables() {
        if !demo_exists() {
            return;
        }

        let sql = r#"
            SELECT tick, 'Damage' as event_type FROM DamageEvent WHERE tick < 10000
            UNION ALL
            SELECT tick, 'BulletHit' as event_type FROM BulletHitEvent WHERE tick < 10000
            UNION ALL
            SELECT tick, 'HeroKilled' as event_type FROM HeroKilledEvent WHERE tick < 10000
        "#;

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run_single_query(sql),
        )
        .await;

        match result {
            Ok(batches) => {
                let rows = total_rows(&batches);
                eprintln!("[test] UNION ALL events returned {} rows", rows);
            }
            Err(_) => panic!("TIMEOUT after 60 seconds"),
        }
    }
}
