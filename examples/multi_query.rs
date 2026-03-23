//! Multi-query example: track heroes AND damage events concurrently.
//!
//! Demonstrates the required pattern for consuming multiple query streams.
//! All streams MUST be drained concurrently to avoid deadlock.
//!
//! Usage:
//!   cargo run --example multi_query -- path/to/demo.dem

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, StringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use demofusion::demo::DemoSource;
use demofusion::session::{IntoStreamingSession, QueryHandle};
use futures::StreamExt;
use tokio::sync::Mutex;

const HERO_QUERY: &str = r#"
SELECT 
    tick,
    entity_index,
    delta_type,
    "m_iTeamNum",
    "m_iHealth"
FROM CCitadelPlayerPawn
WHERE delta_type IN ('create', 'update', 'delete')
"#;

const DAMAGE_QUERY: &str = r#"
SELECT 
    tick,
    damage,
    entindex_victim,
    entindex_attacker
FROM DamageEvent
"#;

#[derive(Default)]
struct GameState {
    heroes_alive: HashMap<i32, &'static str>,
    hero_deaths: HashMap<&'static str, i32>,
    total_damage: HashMap<&'static str, i64>,
    last_tick: i32,
}

async fn process_heroes(mut query: QueryHandle, state: Arc<Mutex<GameState>>) {
    while let Some(result) = query.next().await {
        let batch: RecordBatch = match result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Hero query error: {}", e);
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

        let mut state = state.lock().await;

        for i in 0..batch.num_rows() {
            let tick = ticks.value(i);
            let entity_idx = entity_indices.value(i);
            let delta = delta_types.value(i);
            let team: &'static str = if teams.value(i) == 2 {
                "Amber"
            } else {
                "Sapphire"
            };

            state.last_tick = state.last_tick.max(tick);

            match delta {
                "create" => {
                    state.heroes_alive.insert(entity_idx, team);
                }
                "delete" => {
                    if state.heroes_alive.remove(&entity_idx).is_some() {
                        *state.hero_deaths.entry(team).or_insert(0) += 1;
                        let game_time = tick as f64 / 64.0;
                        println!("[{:.1}s] HERO DEATH: {}", game_time, team);
                    }
                }
                _ => {}
            }
        }
    }
}

async fn process_damage(mut query: QueryHandle, state: Arc<Mutex<GameState>>) {
    while let Some(result) = query.next().await {
        let batch: RecordBatch = match result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Damage query error: {}", e);
                continue;
            }
        };

        let ticks = batch
            .column_by_name("tick")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let damages = batch
            .column_by_name("damage")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let mut state = state.lock().await;

        for i in 0..batch.num_rows() {
            let tick = ticks.value(i);
            let damage = damages.value(i);

            state.last_tick = state.last_tick.max(tick);

            // Track total damage (we don't know attacker team from DamageEvent alone,
            // but this shows the pattern)
            *state.total_damage.entry("total").or_insert(0) += damage as i64;
        }
    }
}

async fn print_summary(state: Arc<Mutex<GameState>>) {
    let mut last_summary_tick = 0;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let state = state.lock().await;
        if state.last_tick > 0 && state.last_tick - last_summary_tick >= 1920 {
            last_summary_tick = state.last_tick;
            let game_time = state.last_tick as f64 / 64.0;

            println!("\n=== Summary @ {:.0}s ===", game_time);
            println!(
                "Hero deaths: Amber {}, Sapphire {}",
                state.hero_deaths.get("Amber").unwrap_or(&0),
                state.hero_deaths.get("Sapphire").unwrap_or(&0)
            );
            println!(
                "Total damage dealt: {}",
                state.total_damage.get("total").unwrap_or(&0)
            );
            println!("Heroes alive: {}", state.heroes_alive.len());
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
    let mut session = source.into_session().await?;

    println!(
        "Loaded demo, {} entity tables available",
        session.schemas().count()
    );

    // Create multiple queries
    let hero_query = session.add_query(HERO_QUERY).await?;
    let damage_query = session.add_query(DAMAGE_QUERY).await?;

    let _handle = session.start()?;

    let state = Arc::new(Mutex::new(GameState::default()));

    // Create summary printer task
    let summary_state = state.clone();
    let summary_task = tokio::spawn(async move {
        print_summary(summary_state).await;
    });

    // CRITICAL: Must consume ALL queries concurrently
    // Sequential consumption will deadlock because the demo
    // parser feeds all streams from a single source
    let hero_state = state.clone();
    let damage_state = state.clone();

    tokio::select! {
        _ = async {
            tokio::join!(
                process_heroes(hero_query, hero_state),
                process_damage(damage_query, damage_state),
            );
        } => {}
    }

    summary_task.abort();

    let final_state = state.lock().await;
    println!("\n=== Final Stats ===");
    println!(
        "Hero deaths: Amber {}, Sapphire {}",
        final_state.hero_deaths.get("Amber").unwrap_or(&0),
        final_state.hero_deaths.get("Sapphire").unwrap_or(&0)
    );
    println!(
        "Total damage dealt: {}",
        final_state.total_damage.get("total").unwrap_or(&0)
    );

    Ok(())
}
