//! Extract player stats from PlayerController entities.
//!
//! PlayerController tracks cumulative stats (kills, deaths, assists, net worth)
//! while PlayerPawn tracks the physical hero body.
//!
//! Usage:
//!   cargo run --example player_stats -- path/to/demo.dem

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
    "m_steamID",
    "m_PlayerDataGlobal__m_nHeroID",
    "m_nAssignedLane",
    "m_PlayerDataGlobal__m_iPlayerKills",
    "m_PlayerDataGlobal__m_iDeaths",
    "m_PlayerDataGlobal__m_iPlayerAssists",
    "m_PlayerDataGlobal__m_iGoldNetWorth",
    "m_PlayerDataGlobal__m_iLevel"
FROM CCitadelPlayerController
"#;

#[derive(Clone)]
struct PlayerStats {
    hero_id: i32,
    lane: i64,
    team: &'static str,
    kills: i64,
    deaths: i64,
    assists: i64,
    net_worth: i64,
    level: i64,
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

    let mut players: HashMap<i32, PlayerStats> = HashMap::new();
    let mut last_print_tick = 0;

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
        let delta_types = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let teams = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let hero_ids = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let lanes = batch
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kills = batch
            .column(7)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let deaths = batch
            .column(8)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let assists = batch
            .column(9)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let net_worths = batch
            .column(10)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let levels = batch
            .column(11)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut current_tick = last_print_tick;

        for i in 0..batch.num_rows() {
            let tick = ticks.value(i);
            let entity_idx = entity_indices.value(i);
            let delta = delta_types.value(i);

            current_tick = current_tick.max(tick);

            if delta == "create" || delta == "update" {
                players.insert(
                    entity_idx,
                    PlayerStats {
                        hero_id: hero_ids.value(i),
                        lane: lanes.value(i),
                        team: if teams.value(i) == 2 {
                            "Amber"
                        } else {
                            "Sapphire"
                        },
                        kills: kills.value(i),
                        deaths: deaths.value(i),
                        assists: assists.value(i),
                        net_worth: net_worths.value(i),
                        level: levels.value(i),
                    },
                );
            }
        }

        // Print scoreboard every ~30 seconds (1920 ticks)
        if current_tick - last_print_tick >= 1920 {
            last_print_tick = current_tick;
            let game_time = current_tick as f64 / 64.0;

            println!("\n=== Scoreboard @ {:.0}s ===", game_time);

            let mut sorted_players: Vec<_> = players.values().cloned().collect();
            sorted_players.sort_by(|a, b| a.team.cmp(b.team).then(a.lane.cmp(&b.lane)));

            for team in ["Amber", "Sapphire"] {
                let team_players: Vec<_> =
                    sorted_players.iter().filter(|p| p.team == team).collect();
                if team_players.is_empty() {
                    continue;
                }

                println!("\n{} Team:", team);
                println!(
                    "{:<5} {:<8} {:<12} {:<10} {:<6}",
                    "Lane", "Hero", "K/D/A", "Net Worth", "Level"
                );
                println!("{}", "-".repeat(45));

                for p in team_players {
                    let kda = format!("{}/{}/{}", p.kills, p.deaths, p.assists);
                    println!(
                        "{:<5} {:<8} {:<12} {:<10} {:<6}",
                        p.lane, p.hero_id, kda, p.net_worth, p.level
                    );
                }
            }
        }
    }

    Ok(())
}
