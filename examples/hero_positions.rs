//! Track hero positions over time.
//!
//! Outputs hero position data for heatmap visualization or pathing analysis.
//!
//! Usage:
//!   cargo run --example hero_positions -- path/to/demo.dem

use std::env;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, UInt64Array};
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
    "CBodyComponent__m_cellY",
    "CBodyComponent__m_cellZ"
FROM CCitadelPlayerPawn
WHERE delta_type IN ('create', 'update')
"#;

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
    let mut session = source.into_session().await?;

    println!(
        "Loaded demo, {} entity tables available",
        session.schemas().count()
    );

    let mut query = session.add_query(QUERY).await?;
    let _handle = session.start()?;

    let mut row_count = 0;

    while let Some(result) = query.next().await {
        let batch = result?;

        let ticks = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let entity_indices = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let teams = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let health = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cell_x = batch
            .column(6)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let cell_y = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let tick = ticks.value(i);
            let entity_idx = entity_indices.value(i);
            let team = if teams.value(i) == 2 {
                "Amber"
            } else {
                "Sapphire"
            };
            let hp = health.value(i);
            let pos_x = cell_to_world(cell_x.value(i));
            let pos_y = cell_to_world(cell_y.value(i));

            row_count += 1;

            if row_count <= 100 {
                println!(
                    "tick={:<6} entity={:<4} team={:<8} hp={:<4} pos=({:.0}, {:.0})",
                    tick, entity_idx, team, hp, pos_x, pos_y
                );
            }
        }
    }

    println!("\nCollected {} position updates", row_count);
    Ok(())
}
