//! A terminal dashboard for a running AetherMesh controller.
//!
//! ```text
//! aether-tui --controller 127.0.0.1:7100
//! ```
//!
//! It polls the client API and draws what the mesh is moving, what it did not
//! have to move, and which node holds what. Tasks can be sent from here, which
//! is the shortest way to watch a placement decision happen.

use std::io;
use std::time::{Duration, Instant};

use anyhow::Context as _;
use clap::Parser;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::crossterm::{cursor, execute};

use aether_tui::app::{App, Key, LineKind, LinkState, Mode, Submission};
use aether_tui::ui;
use aether_tui::{Connection, ConnectionError, SubmitOptions};

/// How long to wait for one reply before calling the controller unresponsive.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait between reconnection attempts once the link has dropped.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

#[derive(Parser)]
#[command(
    name = "aether-tui",
    about = "Terminal dashboard for an AetherMesh mesh"
)]
struct Args {
    /// Controller client API to watch.
    #[arg(long, default_value = "127.0.0.1:7100")]
    controller: String,

    /// Shared secret, when the controller requires one.
    #[arg(long, env = "AETHERMESH_TOKEN")]
    token: Option<String>,

    /// Seconds between polls. Adjustable at runtime with + and -.
    #[arg(long, default_value_t = 1.0)]
    poll_secs: f32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let app = App::new(
        args.controller.clone(),
        Duration::from_secs_f32(args.poll_secs.max(0.05)),
    );

    let mut terminal = enter().context("preparing the terminal")?;
    // The terminal is restored whatever happens next: a panic that leaves the
    // shell in raw mode with no cursor is a worse bug than whatever caused it.
    let outcome = run(&mut terminal, app, args.token).await;
    leave(&mut terminal)?;
    outcome
}

fn enter() -> anyhow::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, cursor::Hide)?;
    Ok(Terminal::new(CrosstermBackend::new(stdout))?)
}

fn leave(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> anyhow::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, cursor::Show)?;
    terminal.show_cursor()?;
    Ok(())
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    mut app: App,
    token: Option<String>,
) -> anyhow::Result<()> {
    let mut client: Option<Connection> = None;
    let mut next_poll = Instant::now();
    let mut next_attempt = Instant::now();

    loop {
        terminal.draw(|frame| ui::draw(frame, &app))?;

        if let Some(submission) = read_key(&mut app)? {
            match client.as_mut() {
                Some(connection) => {
                    let options = SubmitOptions::default()
                        .with_constraints(submission.constraints)
                        .with_priority(submission.priority);
                    match connection
                        .submit(&submission.kind, submission.payload, &options)
                        .await
                    {
                        Ok(finished) => app.apply_result(finished),
                        Err(error) => {
                            app.submitting = false;
                            app.push_log(error.to_string(), LineKind::Bad);
                            // A refusal is the controller answering, not the
                            // link failing; only drop the connection if the
                            // link itself is what broke.
                            if !matches!(error, ConnectionError::Refused(_)) {
                                client = None;
                            }
                        }
                    }
                }
                None => {
                    app.submitting = false;
                    app.push_log("not connected to a controller", LineKind::Bad);
                }
            }
        }
        if app.quit {
            return Ok(());
        }

        let now = Instant::now();
        if client.is_none() && now >= next_attempt {
            next_attempt = now + RECONNECT_DELAY;
            match Connection::connect(&app.addr, token.clone(), REQUEST_TIMEOUT).await {
                Ok(connection) => {
                    client = Some(connection);
                    app.connection = LinkState::Live;
                    app.push_log(format!("connected to {}", app.addr), LineKind::Good);
                    next_poll = now;
                }
                Err(error) => app.lose(error.to_string()),
            }
        }

        if let Some(connection) = client.as_mut()
            && (now >= next_poll || app.refresh)
        {
            app.refresh = false;
            next_poll = now + app.poll;
            match poll(connection, &mut app).await {
                Ok(()) => {}
                Err(error) => {
                    app.lose(error.to_string());
                    // Drop the connection rather than retrying on a socket that
                    // may be half-closed; reconnecting is cheap.
                    client = None;
                }
            }
        }
    }
}

/// One round of questions.
async fn poll(client: &mut Connection, app: &mut App) -> anyhow::Result<()> {
    app.apply_stats(client.stats().await?, Instant::now());
    app.apply_nodes(client.nodes().await?);
    Ok(())
}

/// Handles whatever the operator typed. Returns a task if one is ready to send.
///
/// Polling is on a timer, so this waits only until the next frame is due —
/// a dashboard that ignores keys for a second feels broken.
fn read_key(app: &mut App) -> anyhow::Result<Option<Submission>> {
    if !event::poll(Duration::from_millis(60))? {
        return Ok(None);
    }

    let Event::Key(key) = event::read()? else {
        return Ok(None);
    };
    // Windows reports press and release; acting on both types every character
    // twice.
    if key.kind != KeyEventKind::Press {
        return Ok(None);
    }

    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        app.quit = true;
        return Ok(None);
    }

    match app.mode {
        Mode::Submitting => Ok(match key.code {
            KeyCode::Esc => app.edit_form(Key::Escape),
            KeyCode::Tab | KeyCode::Down => app.edit_form(Key::Tab),
            KeyCode::Left => app.edit_form(Key::Left),
            KeyCode::Right => app.edit_form(Key::Right),
            KeyCode::Enter => app.edit_form(Key::Enter),
            KeyCode::Backspace => app.edit_form(Key::Backspace),
            KeyCode::Char(character) => app.edit_form(Key::Char(character)),
            _ => None,
        }),
        Mode::Help => {
            app.mode = Mode::Watching;
            Ok(None)
        }
        Mode::Watching => {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => app.quit = true,
                KeyCode::Char('s') => app.open_form(),
                KeyCode::Char('?') | KeyCode::Char('h') => app.mode = Mode::Help,
                KeyCode::Down | KeyCode::Char('j') => app.select_next(),
                KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
                KeyCode::Char('+') | KeyCode::Char('=') => app.poll_faster(),
                KeyCode::Char('-') | KeyCode::Char('_') => app.poll_slower(),
                KeyCode::Char('r') => app.refresh = true,
                _ => {}
            }
            Ok(None)
        }
    }
}
