//! Track objective health (Guardian, Walker, Patron).
//!
//! Monitor objective damage and destruction events. Useful for detecting
//! objective pushes, team fights at objectives, and game-ending sequences.
//!
//! Usage:
//!   cargo run --example objectives -- path/to/demo.dem

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use demofusion::demo::DemoSource;
use demofusion::session::{IntoStreamingSession, QueryHandle};
use futures::StreamExt;
use tokio::sync::Mutex;

const GUARDIAN_QUERY: &str = r#"
SELECT 
    tick,
    entity_index,
    delta_type,
    "m_iTeamNum",
    "m_iHealth",
    "m_iMaxHealth",
    "CBodyComponent__m_cellX",
    "CBodyComponent__m_cellY"
FROM CNPC_TrooperBoss
"#;

const WALKER_QUERY: &str = r#"
SELECT 
    tick,
    entity_index,
    delta_type,
    "m_iTeamNum",
    "m_iHealth",
    "m_iMaxHealth",
    "CBodyComponent__m_cellX",
    "CBodyComponent__m_cellY"
FROM CNPC_Boss_Tier2
"#;

const PATRON_QUERY: &str = r#"
SELECT 
    tick,
    entity_index,
    delta_type,
    "m_iTeamNum",
    "m_iHealth",
    "m_iMaxHealth"
FROM CNPC_Boss_Tier3
"#;

struct ObjectiveInfo {
    team: u64,
    health: i64,
    max_health: i64,
}

type ObjectiveKey = (&'static str, i32);
type Objectives = Arc<Mutex<HashMap<ObjectiveKey, ObjectiveInfo>>>;

async fn track_objectives(mut query: QueryHandle, obj_type: &'static str, objectives: Objectives) {
    while let Some(result) = query.next().await {
        let batch: RecordBatch = match result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("{} query error: {}", obj_type, e);
                continue;
            }
        };

        let ticks = batch
            .column_by_name("tick")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let entity_indices = batch
            .column_by_name("entity_index")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let delta_types = batch
            .column_by_name("delta_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let teams = batch
            .column_by_name("m_iTeamNum")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let health_col = batch
            .column_by_name("m_iHealth")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let max_health_col = batch
            .column_by_name("m_iMaxHealth")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut objectives = objectives.lock().await;

        for i in 0..batch.num_rows() {
            let tick = ticks.value(i);
            let entity_idx = entity_indices.value(i);
            let delta = delta_types.value(i);
            let team = teams.value(i);
            let health = health_col.value(i);
            let max_health = max_health_col.value(i);
            let game_seconds = tick as f64 / 64.0;

            let key: ObjectiveKey = (obj_type, entity_idx);

            match delta {
                "create" => {
                    objectives.insert(
                        key,
                        ObjectiveInfo {
                            team,
                            health,
                            max_health,
                        },
                    );
                    println!(
                        "[{:.1}s] {} spawned: team {}, {}/{} HP",
                        game_seconds, obj_type, team, health, max_health
                    );
                }
                "delete" => {
                    println!(
                        "[{:.1}s] {} DESTROYED: team {}",
                        game_seconds, obj_type, team
                    );
                    objectives.remove(&key);
                }
                "update" => {
                    if let Some(obj) = objectives.get_mut(&key) {
                        let old_health = obj.health;
                        if health != old_health {
                            let damage = old_health - health;
                            let pct = if max_health > 0 {
                                health as f64 / max_health as f64 * 100.0
                            } else {
                                0.0
                            };
                            println!(
                                "[{:.1}s] {} team {}: {} -> {} ({:+} dmg, {:.0}%)",
                                game_seconds, obj_type, team, old_health, health, -damage, pct
                            );
                            obj.health = health;
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <demo_file>", args[0]);
        std::process::exit(1);
    }

    let demo_path = &args[1];
    let demo_bytes = tokio::fs::read(demo_path).await?;

    let source = DemoSource::from_bytes(demo_bytes);
    let (mut session, schemas) = source.into_session().await?;

    println!("Loaded demo, {} entity tables available", schemas.len());

    let guardian_query = session.add_query(GUARDIAN_QUERY).await?;
    let walker_query = session.add_query(WALKER_QUERY).await?;
    let patron_query = session.add_query(PATRON_QUERY).await?;

    let _handle = session.start()?;

    let objectives: Objectives = Arc::new(Mutex::new(HashMap::new()));

    // MUST consume all queries concurrently
    tokio::join!(
        track_objectives(guardian_query, "Guardian", objectives.clone()),
        track_objectives(walker_query, "Walker", objectives.clone()),
        track_objectives(patron_query, "Patron", objectives.clone()),
    );

    Ok(())
}
