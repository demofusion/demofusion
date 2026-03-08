//! Monitor lane control via zipline node ownership.
//!
//! Zipline nodes form chains along each lane. Their team ownership indicates
//! which team controls that section. Track ownership changes to analyze
//! lane pressure and push timing.
//!
//! Usage:
//!   cargo run --example lane_control -- path/to/demo.dem

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
    "CBodyComponent__m_cellX",
    "CBodyComponent__m_cellY"
FROM CCitadelZipLineNode
WHERE delta_type IN ('create', 'update')
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

struct NodeInfo {
    lane: &'static str,
    team: u64,
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

    let mut nodes: HashMap<i32, NodeInfo> = HashMap::new();

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
        let cell_x = batch
            .column_by_name("CBodyComponent__m_cellX")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let tick = ticks.value(i);
            let entity_idx = entity_indices.value(i);
            let delta = delta_types.value(i);
            let team = teams.value(i);
            let pos_x = cell_to_world(cell_x.value(i));
            let lane = get_lane(pos_x);
            let game_seconds = tick as f64 / 64.0;

            match delta {
                "create" => {
                    nodes.insert(entity_idx, NodeInfo { lane, team });
                    println!(
                        "[{:.1}s] Node {} created: {} lane, team {}",
                        game_seconds, entity_idx, lane, team
                    );
                }
                "update" => {
                    if let Some(node) = nodes.get_mut(&entity_idx) {
                        if team != node.team {
                            let old_team = node.team;
                            node.team = team;
                            println!(
                                "[{:.1}s] Node {} flipped: {} lane, team {} -> {}",
                                game_seconds, entity_idx, node.lane, old_team, team
                            );
                        }
                    }
                }
                _ => {}
            }
        }

        // Summary after each batch
        if !nodes.is_empty() {
            let amber = nodes.values().filter(|n| n.team == 2).count();
            let sapphire = nodes.values().filter(|n| n.team == 3).count();
            println!("  Lane control: Amber {}, Sapphire {}", amber, sapphire);
        }
    }

    Ok(())
}
