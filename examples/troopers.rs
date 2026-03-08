//! Analyze trooper waves and deaths.
//!
//! Troopers spawn in squads of 4 and march down lanes. Track their spawns,
//! deaths, and positions to understand lane pressure and farming patterns.
//!
//! Usage:
//!   cargo run --example troopers -- path/to/demo.dem

use std::collections::HashMap;
use std::env;

use datafusion::arrow::array::{Array, Int32Array, StringArray, UInt64Array};
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
FROM CNPC_Trooper
"#;

fn get_lane(x: f64) -> &'static str {
    if x < -1530.0 {
        "Yellow"
    } else if x > 1530.0 {
        "Green"
    } else {
        "Blue"
    }
}

fn cell_to_world(cell: u64) -> f64 {
    (cell as f64 * 128.0) - 16384.0
}

struct TrooperInfo {
    team: &'static str,
    lane: &'static str,
}

#[derive(Default)]
struct Stats {
    spawns_amber: i32,
    spawns_sapphire: i32,
    deaths_amber: i32,
    deaths_sapphire: i32,
    deaths_by_lane: HashMap<&'static str, (i32, i32)>, // (amber, sapphire)
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

    let mut troopers: HashMap<i32, TrooperInfo> = HashMap::new();
    let mut stats = Stats {
        deaths_by_lane: [("Yellow", (0, 0)), ("Blue", (0, 0)), ("Green", (0, 0))]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let mut batch_count = 0;

    while let Some(result) = query.next().await {
        let batch = result?;
        batch_count += 1;

        let entity_indices = batch.column_by_name("entity_index").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        let delta_types = batch.column_by_name("delta_type").unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        let teams = batch.column_by_name("m_iTeamNum").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();
        let cell_x = batch.column_by_name("CBodyComponent__m_cellX").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap();

        for i in 0..batch.num_rows() {
            let entity_idx = entity_indices.value(i);
            let delta = delta_types.value(i);
            let team = if teams.value(i) == 2 { "Amber" } else { "Sapphire" };
            let pos_x = cell_to_world(cell_x.value(i));
            let lane = get_lane(pos_x);

            match delta {
                "create" => {
                    troopers.insert(entity_idx, TrooperInfo { team, lane });
                    if team == "Amber" {
                        stats.spawns_amber += 1;
                    } else {
                        stats.spawns_sapphire += 1;
                    }
                }
                "delete" => {
                    if let Some(trooper) = troopers.remove(&entity_idx) {
                        if trooper.team == "Amber" {
                            stats.deaths_amber += 1;
                        } else {
                            stats.deaths_sapphire += 1;
                        }
                        let lane_stats = stats.deaths_by_lane.entry(trooper.lane).or_insert((0, 0));
                        if trooper.team == "Amber" {
                            lane_stats.0 += 1;
                        } else {
                            lane_stats.1 += 1;
                        }
                    }
                }
                _ => {}
            }
        }

        // Print summary periodically
        if batch_count % 10 == 0 {
            println!("\n=== Trooper Stats (batch {}) ===", batch_count);
            println!("Spawns: Amber {}, Sapphire {}", stats.spawns_amber, stats.spawns_sapphire);
            println!("Deaths: Amber {}, Sapphire {}", stats.deaths_amber, stats.deaths_sapphire);
            println!("Deaths by lane:");
            for lane_name in ["Yellow", "Blue", "Green"] {
                let (amber, sapphire) = stats.deaths_by_lane.get(lane_name).unwrap_or(&(0, 0));
                println!("  {}: Amber {}, Sapphire {}", lane_name, amber, sapphire);
            }
            println!("Active troopers: {}", troopers.len());
        }
    }

    println!("\n=== Final Trooper Stats ===");
    println!("Spawns: Amber {}, Sapphire {}", stats.spawns_amber, stats.spawns_sapphire);
    println!("Deaths: Amber {}, Sapphire {}", stats.deaths_amber, stats.deaths_sapphire);

    Ok(())
}
