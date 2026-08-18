//! Renders one frame of the dashboard against a live controller and prints it.
//! Run with: cargo run -p aether-tui --example snapshot
use std::time::{Duration, Instant};

use aether_tui::Connection;
use aether_tui::app::App;
use aether_tui::ui;
use ratatui::Terminal;
use ratatui::backend::TestBackend;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or("127.0.0.1:7100".into());
    let mut client = Connection::connect(&addr, None, Duration::from_secs(5)).await?;
    let mut app = App::new(addr, Duration::from_secs(1));

    let start = Instant::now();
    app.apply_stats(client.stats().await?, start);
    tokio::time::sleep(Duration::from_millis(400)).await;
    app.apply_stats(client.stats().await?, Instant::now());
    app.apply_nodes(client.nodes().await?);

    let mut terminal = Terminal::new(TestBackend::new(118, 26))?;
    terminal.draw(|frame| ui::draw(frame, &app))?;
    let buffer = terminal.backend().buffer().clone();
    for y in 0..buffer.area.height {
        let row: String = (0..buffer.area.width)
            .map(|x| buffer[(x, y)].symbol().to_string())
            .collect();
        println!("{}", row.trim_end());
    }
    Ok(())
}
