//! Detect combat engagements via health changes.
//!
//! Monitors hero health to detect when combat is happening. Identifies
//! engagements, damage taken, and potential deaths.
//!
//! Usage:
//!   cargo run --example combat_detection -- path/to/demo.dem

use std::collections::HashMap;
use std::env;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray, UInt64Array};
use demofusion::demo::DemoSource;
use demofusion::session::IntoStreamingSession;
use futures::StreamExt;

const QUERY: &str = r#"
SELECT 
    tick,
    entity_index,
    delta_type,
    "m_iTeamNum",
    "m_iHealth",
    "m_iMaxHealth",
    "CBodyComponent__m_cellX",
    "CBodyComponent__m_cellY"
FROM CCitadelPlayerPawn
WHERE delta_type IN ('create', 'update', 'delete')
"#;

struct HeroState {
    team: &'static str,
    health: i64,
    max_health: i64,
    position_x: f64,
    position_y: f64,
    last_damage_tick: i32,
    in_combat: bool,
}

fn cell_to_world(cell: u64) -> f64 {
    (cell as f64 * 128.0) - 16384.0
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

    let mut query = session.add_query(QUERY).await?;
    let _handle = session.start()?;

    let mut heroes: HashMap<i32, HeroState> = HashMap::new();
    let combat_cooldown_ticks = 192; // 3 seconds at 64 ticks/s

    while let Some(result) = query.next().await {
        let batch = result?;

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
        let cell_x = batch
            .column_by_name("CBodyComponent__m_cellX")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let cell_y = batch
            .column_by_name("CBodyComponent__m_cellY")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let tick = ticks.value(i);
            let entity_idx = entity_indices.value(i);
            let delta = delta_types.value(i);
            let team = if teams.value(i) == 2 {
                "Amber"
            } else {
                "Sapphire"
            };
            let health = health_col.value(i);
            let max_health = max_health_col.value(i);
            let pos_x = cell_to_world(cell_x.value(i));
            let pos_y = cell_to_world(cell_y.value(i));
            let game_seconds = tick as f64 / 64.0;

            match delta {
                "create" => {
                    heroes.insert(
                        entity_idx,
                        HeroState {
                            team,
                            health,
                            max_health,
                            position_x: pos_x,
                            position_y: pos_y,
                            last_damage_tick: 0,
                            in_combat: false,
                        },
                    );
                    println!(
                        "[{:.1}s] Hero spawned: entity {}, {}",
                        game_seconds, entity_idx, team
                    );
                }
                "delete" => {
                    if let Some(hero) = heroes.remove(&entity_idx) {
                        println!(
                            "[{:.1}s] Hero DIED: entity {}, {} at ({:.0}, {:.0})",
                            game_seconds, entity_idx, hero.team, hero.position_x, hero.position_y
                        );
                    }
                }
                "update" => {
                    if let Some(hero) = heroes.get_mut(&entity_idx) {
                        let old_health = hero.health;
                        hero.health = health;
                        hero.max_health = max_health;
                        hero.position_x = pos_x;
                        hero.position_y = pos_y;

                        if health < old_health {
                            let damage = old_health - health;
                            let pct = if hero.max_health > 0 {
                                health as f64 / hero.max_health as f64 * 100.0
                            } else {
                                0.0
                            };

                            if !hero.in_combat {
                                hero.in_combat = true;
                                println!(
                                    "[{:.1}s] COMBAT START: {} hero at ({:.0}, {:.0})",
                                    game_seconds, hero.team, hero.position_x, hero.position_y
                                );
                            }

                            hero.last_damage_tick = tick;
                            println!(
                                "[{:.1}s] {} took {} dmg ({:.0}% HP remaining)",
                                game_seconds, hero.team, damage, pct
                            );
                        } else if hero.in_combat
                            && tick - hero.last_damage_tick > combat_cooldown_ticks
                        {
                            hero.in_combat = false;
                            println!(
                                "[{:.1}s] COMBAT END: {} hero disengaged",
                                game_seconds, hero.team
                            );
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}
