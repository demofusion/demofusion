//! Join damage events with hero positions to analyze combat hotspots.
//!
//! Demonstrates streaming JOINs by correlating DamageEvent with
//! CCitadelPlayerPawn positions at each tick. Shows where damage
//! is happening on the map.
//!
//! Usage:
//!   cargo run --release --example damage_locations -- path/to/demo.dem

use std::collections::HashMap;
use std::env;

use datafusion::arrow::array::{Array, Int32Array, UInt64Array};
use demofusion::demo::DemoSource;
use demofusion::session::IntoStreamingSession;
use futures::StreamExt;

const QUERY: &str = r#"
SELECT 
    d.tick,
    d.damage,
    d.entindex_victim,
    d.entindex_attacker,
    p.entity_index,
    p."m_iTeamNum",
    p."CBodyComponent__m_cellX",
    p."CBodyComponent__m_cellY"
FROM DamageEvent d
INNER JOIN CCitadelPlayerPawn p ON d.tick = p.tick
"#;

struct GridCell {
    damage_dealt: i64,
    damage_events: u32,
}

fn cell_to_grid(cell_x: u64, cell_y: u64) -> (i32, i32) {
    let world_x = (cell_x as f64 * 128.0) - 16384.0;
    let world_y = (cell_y as f64 * 128.0) - 16384.0;
    let grid_x = (world_x / 1000.0).floor() as i32;
    let grid_y = (world_y / 1000.0).floor() as i32;
    (grid_x, grid_y)
}

fn grid_to_quadrant(grid_x: i32, grid_y: i32) -> &'static str {
    match (grid_x < -13, grid_y < -13) {
        (true, true) => "Northwest",
        (true, false) => "Southwest",
        (false, true) => "Northeast",
        (false, false) => "Southeast",
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
    let (mut session, _schemas) = source.into_session().await?;

    println!("Analyzing damage locations via JOIN query...\n");
    println!("Query:\n{}\n", QUERY.trim());

    let mut query = session.add_query(QUERY).await?;
    let _handle = session.start()?;

    let mut hotspots: HashMap<(i32, i32), GridCell> = HashMap::new();
    let mut total_damage: i64 = 0;
    let mut total_events: u64 = 0;
    let mut last_tick = 0i32;

    while let Some(result) = query.next().await {
        let batch = result?;

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
        let victim_indices = batch
            .column_by_name("entindex_victim")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let pawn_indices = batch
            .column_by_name("entity_index")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
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
            let damage = damages.value(i);
            let victim_idx = victim_indices.value(i);
            let pawn_idx = pawn_indices.value(i);

            if victim_idx == pawn_idx && damage > 0 {
                let grid = cell_to_grid(cell_x.value(i), cell_y.value(i));

                let cell = hotspots.entry(grid).or_insert(GridCell {
                    damage_dealt: 0,
                    damage_events: 0,
                });
                cell.damage_dealt += damage as i64;
                cell.damage_events += 1;

                total_damage += damage as i64;
                total_events += 1;
                last_tick = tick;
            }
        }
    }

    println!("=== Damage Hotspot Analysis ===\n");
    println!(
        "Total damage tracked: {} across {} events",
        total_damage, total_events
    );
    println!(
        "Game duration: {:.1} seconds\n",
        last_tick as f64 / 64.0
    );

    let mut by_location: HashMap<&'static str, (i64, u32)> = HashMap::new();
    for ((gx, gy), cell) in &hotspots {
        let location = grid_to_quadrant(*gx, *gy);
        let entry = by_location.entry(location).or_insert((0, 0));
        entry.0 += cell.damage_dealt;
        entry.1 += cell.damage_events;
    }

    let mut sorted: Vec<_> = by_location.into_iter().collect();
    sorted.sort_by(|a, b| b.1 .0.cmp(&a.1 .0));

    println!("{:<20} {:>12} {:>10}", "Location", "Damage", "Events");
    println!("{}", "-".repeat(44));
    for (location, (damage, events)) in sorted {
        let pct = if total_damage > 0 {
            damage as f64 / total_damage as f64 * 100.0
        } else {
            0.0
        };
        println!(
            "{:<20} {:>12} {:>9} ({:.1}%)",
            location, damage, events, pct
        );
    }

    println!("\n=== Top 5 Grid Cells by Damage ===\n");
    let mut grid_sorted: Vec<_> = hotspots.into_iter().collect();
    grid_sorted.sort_by(|a, b| b.1.damage_dealt.cmp(&a.1.damage_dealt));

    println!("{:<12} {:>12} {:>10}", "Grid (x,y)", "Damage", "Events");
    println!("{}", "-".repeat(36));
    for ((gx, gy), cell) in grid_sorted.into_iter().take(5) {
        println!(
            "({:>3}, {:>3})   {:>12} {:>10}",
            gx, gy, cell.damage_dealt, cell.damage_events
        );
    }

    Ok(())
}
