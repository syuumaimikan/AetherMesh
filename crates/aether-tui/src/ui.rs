//! Drawing the dashboard.
//!
//! One screen, no scrolling, no menus. What an operator wants to know is
//! "is anything moving, where is it going, and did the mesh save me anything" —
//! three questions that fit side by side, so they are side by side.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line as TextLine, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Sparkline, Table, Wrap};

use crate::app::{App, Connection, Field, LineKind, Mode, bytes, short};

const ACCENT: Color = Color::Cyan;
const GOOD: Color = Color::Green;
const BAD: Color = Color::Red;
const DIM: Color = Color::DarkGray;

pub fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // status bar
            Constraint::Length(9), // traffic and counters
            Constraint::Min(6),    // nodes
            Constraint::Length(7), // activity
            Constraint::Length(1), // keys
        ])
        .split(area);

    status(frame, rows[0], app);
    top(frame, rows[1], app);
    nodes(frame, rows[2], app);
    activity(frame, rows[3], app);
    keys(frame, rows[4], app);

    match app.mode {
        Mode::Submitting => form(frame, area, app),
        Mode::Help => help(frame, area),
        Mode::Watching => {}
    }
}

fn status(frame: &mut Frame, area: Rect, app: &App) {
    let (mark, text, colour) = match &app.connection {
        Connection::Connecting => ("○", "connecting".to_string(), Color::Yellow),
        Connection::Live => ("●", "live".to_string(), GOOD),
        Connection::Lost(reason) => ("✕", format!("lost — {reason}"), BAD),
    };

    let line = TextLine::from(vec![
        Span::styled(
            " AetherMesh ",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Span::styled(format!("{mark} "), Style::default().fg(colour)),
        Span::styled(text, Style::default().fg(colour)),
        Span::styled(format!("  {}", app.addr), Style::default().fg(DIM)),
        Span::styled(
            format!("  every {:.2}s", app.poll.as_secs_f32()),
            Style::default().fg(DIM),
        ),
    ]);
    frame.render_widget(Paragraph::new(line), area);
}

/// Traffic on the left, what it saved in the middle, mesh counters on the right.
fn top(frame: &mut Frame, area: Rect, app: &App) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            // Narrow enough that the widest label in the other two panels
            // ("transfers skipped") still has room for its number beside it.
            Constraint::Percentage(36),
            Constraint::Percentage(32),
            Constraint::Percentage(32),
        ])
        .split(area);

    throughput(frame, columns[0], app);
    savings(frame, columns[1], app);
    counters(frame, columns[2], app);
}

fn throughput(frame: &mut Frame, area: Rect, app: &App) {
    let inner = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(block(frame, area, " Throughput "));

    let latest = app.throughput.latest();
    let peak = app.throughput.peak();
    frame.render_widget(
        Paragraph::new(vec![
            TextLine::from(vec![
                Span::styled(
                    format!("{}/s", bytes(latest)),
                    Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("   peak {}/s", bytes(peak)),
                    Style::default().fg(DIM),
                ),
            ]),
            TextLine::from(Span::styled(
                match app.traffic {
                    Some(traffic) => format!("{} on the wire so far", bytes(traffic.bytes_sent)),
                    None => "waiting for the first sample".to_string(),
                },
                Style::default().fg(DIM),
            )),
        ]),
        inner[0],
    );

    let samples = app.throughput.samples();
    frame.render_widget(
        Sparkline::default()
            .data(&samples)
            .style(Style::default().fg(ACCENT)),
        inner[1],
    );
}

/// The number the whole project exists to produce.
fn savings(frame: &mut Frame, area: Rect, app: &App) {
    let inner = block(frame, area, " Not moved ");
    let Some(traffic) = app.traffic else {
        frame.render_widget(waiting(), inner);
        return;
    };

    frame.render_widget(
        Paragraph::new(vec![
            pair(
                "compressed away",
                bytes(traffic.bytes_saved_by_compression),
                GOOD,
            ),
            pair(
                "ratio",
                match traffic.bytes_uncompressed {
                    0 => "—".to_string(),
                    _ => format!("{:.3}", traffic.compression_ratio),
                },
                ACCENT,
            ),
            pair(
                "transfers skipped",
                traffic.transfers_skipped.to_string(),
                GOOD,
            ),
            pair("chunks skipped", traffic.chunks_skipped.to_string(), GOOD),
            pair(
                "retries",
                traffic.retries.to_string(),
                if traffic.retries == 0 {
                    DIM
                } else {
                    Color::Yellow
                },
            ),
        ]),
        inner,
    );
}

fn counters(frame: &mut Frame, area: Rect, app: &App) {
    let inner = block(frame, area, " Mesh ");
    let Some(mesh) = app.mesh else {
        frame.render_widget(waiting(), inner);
        return;
    };

    frame.render_widget(
        Paragraph::new(vec![
            pair(
                "nodes",
                format!(
                    "{}/{} connected",
                    app.totals.nodes_connected, app.totals.nodes
                ),
                if app.totals.nodes_connected == app.totals.nodes {
                    GOOD
                } else {
                    Color::Yellow
                },
            ),
            pair(
                "datasets",
                format!(
                    "{} · {}",
                    app.totals.datasets,
                    bytes(app.totals.dataset_bytes)
                ),
                ACCENT,
            ),
            pair("tasks ok", mesh.tasks_completed.to_string(), GOOD),
            pair(
                "tasks failed",
                mesh.tasks_failed.to_string(),
                if mesh.tasks_failed == 0 { DIM } else { BAD },
            ),
            pair(
                "evicted",
                mesh.nodes_evicted.to_string(),
                if mesh.nodes_evicted == 0 {
                    DIM
                } else {
                    Color::Yellow
                },
            ),
        ]),
        inner,
    );
}

fn nodes(frame: &mut Frame, area: Rect, app: &App) {
    let inner = block(frame, area, &format!(" Nodes ({}) ", app.nodes.len()));

    if app.nodes.is_empty() {
        frame.render_widget(
            Paragraph::new("no nodes registered — start an agent")
                .style(Style::default().fg(DIM))
                .alignment(Alignment::Center),
            inner,
        );
        return;
    }

    let header = Row::new(
        [
            "", "host", "id", "cpu", "mem", "rtt", "link", "holds", "labels",
        ]
        .map(|title| Cell::from(title).style(Style::default().fg(DIM))),
    );

    let rows = app.nodes.iter().enumerate().map(|(index, node)| {
        let selected = index == app.selected;
        let style = if selected {
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
        } else if node.connected {
            Style::default()
        } else {
            Style::default().fg(DIM)
        };

        Row::new(vec![
            Cell::from(if node.connected { "●" } else { "○" })
                .style(Style::default().fg(if node.connected { GOOD } else { BAD })),
            Cell::from(node.hostname.clone()),
            Cell::from(short(&node.node_id).to_string()),
            Cell::from(percent(node.cpu_usage)),
            Cell::from(percent(node.memory_usage)),
            Cell::from(match node.latency_ms {
                Some(ms) => format!("{ms:.1} ms"),
                None => "—".to_string(),
            }),
            Cell::from(match node.bandwidth_bytes_per_sec {
                Some(rate) => format!("{}/s", bytes(rate)),
                None => "—".to_string(),
            }),
            // The locality column: work reading these costs no transfer, which
            // is the decision the scheduler is making on every task.
            Cell::from(match node.datasets_held {
                0 => "—".to_string(),
                count => format!("{count} · {}", bytes(node.bytes_held)),
            })
            .style(Style::default().fg(if node.datasets_held > 0 {
                GOOD
            } else {
                DIM
            })),
            Cell::from(
                node.labels
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(" "),
            )
            .style(Style::default().fg(DIM)),
        ])
        .style(style)
    });

    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Length(2),
                Constraint::Min(10),
                Constraint::Length(9),
                Constraint::Length(6),
                Constraint::Length(6),
                Constraint::Length(9),
                Constraint::Length(12),
                Constraint::Length(16),
                Constraint::Min(12),
            ],
        )
        .header(header),
        inner,
    );
}

fn activity(frame: &mut Frame, area: Rect, app: &App) {
    let inner = block(frame, area, " Activity ");
    if app.log.is_empty() {
        frame.render_widget(
            Paragraph::new("press s to send a task").style(Style::default().fg(DIM)),
            inner,
        );
        return;
    }

    let lines: Vec<TextLine> = app
        .log
        .iter()
        .take(inner.height as usize)
        .map(|line| {
            TextLine::from(Span::styled(
                line.text.clone(),
                Style::default().fg(match line.kind {
                    LineKind::Info => Color::Reset,
                    LineKind::Good => GOOD,
                    LineKind::Bad => BAD,
                }),
            ))
        })
        .collect();

    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), inner);
}

fn keys(frame: &mut Frame, area: Rect, app: &App) {
    let text = match app.mode {
        Mode::Submitting => " tab field   enter send   esc cancel ",
        Mode::Help => " any key closes ",
        Mode::Watching => " q quit   s send a task   ↑↓ node   +/- poll rate   r refresh   ? help ",
    };
    frame.render_widget(
        Paragraph::new(Span::styled(text, Style::default().fg(DIM))),
        area,
    );
}

fn form(frame: &mut Frame, area: Rect, app: &App) {
    let popup = centred(area, 60, 9);
    frame.render_widget(Clear, popup);
    let inner = block(frame, popup, " Send a task ");

    let field = |label: &str, value: &str, focused: bool| {
        let marker = if focused { "▸ " } else { "  " };
        TextLine::from(vec![
            Span::styled(
                format!("{marker}{label:<12}"),
                Style::default().fg(if focused { ACCENT } else { DIM }),
            ),
            Span::styled(
                if focused {
                    format!("{value}_")
                } else {
                    value.to_string()
                },
                Style::default().fg(Color::Reset),
            ),
        ])
    };

    frame.render_widget(
        Paragraph::new(vec![
            field("kind", &app.form.kind, app.form.focus == Field::Kind),
            field(
                "payload",
                &app.form.payload,
                app.form.focus == Field::Payload,
            ),
            field(
                "constraints",
                &app.form.constraints,
                app.form.focus == Field::Constraints,
            ),
            TextLine::from(""),
            TextLine::from(Span::styled(
                "  echo · hash · cpu (payload is an iteration count)",
                Style::default().fg(DIM),
            )),
            TextLine::from(Span::styled(
                "  constraints: gpu=true, region!=us-east, nvme",
                Style::default().fg(DIM),
            )),
        ]),
        inner,
    );
}

fn help(frame: &mut Frame, area: Rect) {
    let popup = centred(area, 64, 14);
    frame.render_widget(Clear, popup);
    let inner = block(frame, popup, " What this shows ");

    frame.render_widget(
        Paragraph::new(vec![
            TextLine::from("Throughput   bytes actually written to sockets, per second."),
            TextLine::from("Not moved    what the mesh did not have to send: compression,"),
            TextLine::from("             whole datasets a node already had, and chunks"),
            TextLine::from("             deduplicated against data it already held."),
            TextLine::from(""),
            TextLine::from("holds        datasets that node already has. Work reading"),
            TextLine::from("             them costs no transfer, which is the decision"),
            TextLine::from("             the scheduler makes on every task."),
            TextLine::from(""),
            TextLine::from("A node can be registered and not connected: the registry"),
            TextLine::from("keeps it until its heartbeat times out, deliberately."),
        ])
        .wrap(Wrap { trim: false }),
        inner,
    );
}

/// Draws a titled border and returns the area inside it.
fn block(frame: &mut Frame, area: Rect, title: &str) -> Rect {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(DIM))
        .title(Span::styled(title.to_string(), Style::default().fg(ACCENT)));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    inner
}

fn waiting() -> Paragraph<'static> {
    Paragraph::new("waiting for the controller").style(Style::default().fg(DIM))
}

fn pair(label: &str, value: String, colour: Color) -> TextLine<'static> {
    TextLine::from(vec![
        Span::styled(format!("{label:<18}"), Style::default().fg(DIM)),
        Span::styled(value, Style::default().fg(colour)),
    ])
}

fn percent(ratio: f32) -> String {
    format!("{:.0}%", ratio * 100.0)
}

/// A popup of at most `width` by `height`, centred, never larger than the screen.
fn centred(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use aether_controller::client::{ClientResponse, NodeSummary, TrafficSummary};
    use aether_controller::observability::MetricsSnapshot;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;

    fn app_with_data() -> App {
        let mut app = App::new("127.0.0.1:7100".to_string(), Duration::from_secs(1));
        let start = Instant::now();

        app.apply_stats(
            ClientResponse::Stats {
                traffic: TrafficSummary {
                    bytes_sent: 4_127,
                    bytes_uncompressed: 1_048_576,
                    bytes_saved_by_compression: 1_044_449,
                    compression_ratio: 0.0039,
                    transfers_skipped: 2,
                    chunks_skipped: 3,
                    retries: 0,
                },
                mesh: MetricsSnapshot {
                    tasks_completed: 3,
                    ..MetricsSnapshot::default()
                },
                nodes: 1,
                nodes_connected: 1,
                datasets: 2,
                dataset_bytes: 5_242_880,
            },
            start,
        );

        let mut node = NodeSummary {
            node_id: "4f3cb68b-1111-2222".to_string(),
            hostname: "rpi4".to_string(),
            cpu_cores: 4,
            cpu_usage: 0.42,
            memory_usage: 0.61,
            labels: Default::default(),
            address: "10.0.0.4:7001".to_string(),
            latency_ms: Some(4.5),
            bandwidth_bytes_per_sec: Some(12_500_000),
            datasets_held: 2,
            bytes_held: 5_242_880,
            connected: true,
        };
        node.labels.insert("kind".to_string(), "arm".to_string());
        app.apply_nodes(ClientResponse::Nodes { nodes: vec![node] });
        app
    }

    fn render(app: &App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("a test terminal");
        terminal.draw(|frame| draw(frame, app)).expect("a frame");

        let buffer = terminal.backend().buffer().clone();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn the_screen_shows_what_moved_and_what_did_not() {
        let screen = render(&app_with_data(), 120, 30);

        assert!(screen.contains("rpi4"), "{screen}");
        assert!(screen.contains("4f3cb68b"));
        assert!(screen.contains("kind=arm"));
        assert!(screen.contains("4.5 ms"));
        // The saving is the point, so it has to be legible on the screen.
        assert!(
            screen.contains("1020.0 KiB") || screen.contains("1.0 MiB"),
            "{screen}"
        );
        assert!(screen.contains("live"));
    }

    #[test]
    fn an_empty_mesh_says_so_instead_of_showing_an_empty_table() {
        let app = App::new("127.0.0.1:7100".to_string(), Duration::from_secs(1));
        let screen = render(&app, 100, 30);

        assert!(screen.contains("no nodes registered"), "{screen}");
        assert!(screen.contains("connecting"));
    }

    #[test]
    fn a_lost_controller_is_visible_without_hiding_the_last_numbers() {
        let mut app = app_with_data();
        app.lose("connection reset");

        let screen = render(&app, 120, 30);
        assert!(screen.contains("lost"), "{screen}");
        assert!(
            screen.contains("rpi4"),
            "the last known mesh is still shown"
        );
    }

    #[test]
    fn the_form_appears_over_the_dashboard() {
        let mut app = app_with_data();
        app.open_form();

        let screen = render(&app, 120, 30);
        assert!(screen.contains("Send a task"), "{screen}");
        assert!(screen.contains("constraints"));
        assert!(screen.contains("enter send"));
    }

    #[test]
    fn help_explains_the_columns_that_are_not_obvious() {
        let mut app = app_with_data();
        app.mode = Mode::Help;

        let screen = render(&app, 120, 30);
        assert!(screen.contains("deduplicated"), "{screen}");
    }

    #[test]
    fn a_small_terminal_still_renders() {
        // A popup wider than the screen would panic on the subtraction in
        // `centred`, and an operator on a split pane should not find that out.
        let mut app = app_with_data();
        app.open_form();

        for (width, height) in [(40, 12), (20, 8), (80, 24)] {
            let screen = render(&app, width, height);
            assert!(!screen.is_empty());
        }
    }
}
