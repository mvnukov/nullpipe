use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, Subcommand};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ephemeral_chat_core::{host, join, ChatEvent, HostConfig, JoinConfig, PeerInfo, RoomHandle};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Paragraph},
    Frame, Terminal,
};
use tokio::sync::mpsc;

const DEFAULT_INVITE_TTL: u64 = 300;
const CONFIG_DIR_NAME: &str = "ephemeral-chat";
const NAME_FILE: &str = "name";
const APP_NAME: &str = "ephemeral-chat";
const MAX_DISPLAY_NAME: usize = 20;

const SPINNER: &[char] = &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
const TICK_MS: u64 = 100;

// ---------------------------------------------------------------------------
// Async command results (sent from spawned tasks back to main loop)
// ---------------------------------------------------------------------------

enum CmdResult {
    Invite { code: Result<String, String> },
    Peers { peers: Vec<PeerInfo> },
    Quit,
}

// ---------------------------------------------------------------------------
// Display message
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct DispMsg {
    ts: Option<chrono::DateTime<chrono::Local>>,
    name: String,
    text: String,
    system: bool,
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(PartialEq)]
enum Mode {
    Bootstrap { progress: u8, msg: String },
    Running,
    ShuttingDown { since: Instant },
}

struct App {
    mode: Mode,
    handle: Option<RoomHandle>,
    msgs: Vec<DispMsg>,
    input: String,
    cursor: usize,
    scroll: usize, // 0 = at bottom
    onion: Option<String>,
    peers: Vec<PeerInfo>,
    timestamps: bool,
    quit: bool,
    bootstrap_start: Instant,
    input_focused: bool,
    cmd_tx: Option<mpsc::UnboundedSender<CmdResult>>,
}

impl App {
    fn new(_name: String, timestamps: bool) -> Self {
        Self {
            mode: Mode::Bootstrap {
                progress: 0,
                msg: "starting tor...".into(),
            },
            handle: None,
            msgs: Vec::new(),
            input: String::new(),
            cursor: 0,
            scroll: 0,
            onion: None,
            peers: Vec::new(),
            timestamps,
            quit: false,
            bootstrap_start: Instant::now(),
            input_focused: true,
            cmd_tx: None,
        }
    }

    fn set_cmd_tx(&mut self, tx: mpsc::UnboundedSender<CmdResult>) {
        self.cmd_tx = Some(tx);
    }

    fn push(&mut self, name: String, text: String, system: bool) {
        let ts = self.timestamps.then(chrono::Local::now);
        self.msgs.push(DispMsg {
            ts,
            name,
            text,
            system,
        });
    }

    fn at_bottom(&self) -> bool {
        self.scroll == 0
    }

    fn handle_event(&mut self, ev: ChatEvent) {
        match ev {
            ChatEvent::BootstrapProgress(pct) => {
                if let Mode::Bootstrap { progress, msg } = &mut self.mode {
                    *progress = pct;
                    if *progress < 100 {
                        *msg = "bootstrapping tor...".to_string();
                    }
                }
            }
            ChatEvent::RoomReady { onion_address, .. } => {
                self.onion = Some(onion_address.clone());
                self.mode = Mode::Running;
                let truncated = if onion_address.len() > 12 {
                    &onion_address[..12]
                } else {
                    &onion_address
                };
                self.push(
                    "system".into(),
                    format!("room ready: {}...", truncated),
                    true,
                );
            }
            ChatEvent::PeerJoin(info) => {
                if !self.peers.iter().any(|p| p.id == info.id) {
                    self.peers.push(info.clone());
                }
                self.push("system".into(), format!("{} joined", info.name), true);
            }
            ChatEvent::PeerLeave(pid) => {
                self.peers.retain(|p| p.id != pid);
                self.push("system".into(), format!("{} left", pid), true);
            }
            ChatEvent::Message { name, text, .. } => {
                self.push(name, text, false);
            }
            ChatEvent::InviteCreated { code } => {
                self.push("system".into(), format!("new invite code: {}", code), true);
            }
            ChatEvent::RoomClosed => {
                self.push("system".into(), "room closed".into(), true);
                self.mode = Mode::ShuttingDown {
                    since: Instant::now(),
                };
            }
            ChatEvent::Error(e) => {
                self.push("system".into(), format!("error: {}", e), true);
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        // Ctrl-C always triggers quit
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.quit = true;
            return;
        }

        match &self.mode {
            Mode::Bootstrap { .. } | Mode::ShuttingDown { .. } => {
                // input locked
            }
            Mode::Running => match key.code {
                KeyCode::Enter => {
                    self.send();
                }
                KeyCode::Char(c)
                    if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
                {
                    self.input.insert(self.cursor, c);
                    self.cursor += 1;
                }
                KeyCode::Backspace => {
                    if self.cursor > 0 {
                        self.input.remove(self.cursor - 1);
                        self.cursor -= 1;
                    }
                }
                KeyCode::Delete => {
                    if self.cursor < self.input.len() {
                        self.input.remove(self.cursor);
                    }
                }
                KeyCode::Left => {
                    if self.cursor > 0 {
                        self.cursor -= 1;
                    }
                }
                KeyCode::Right => {
                    if self.cursor < self.input.len() {
                        self.cursor += 1;
                    }
                }
                KeyCode::Home => {
                    self.cursor = 0;
                }
                KeyCode::End => {
                    self.cursor = self.input.len();
                }
                KeyCode::Up => {
                    self.scroll += 1;
                }
                KeyCode::Down => {
                    self.scroll = self.scroll.saturating_sub(1);
                }
                KeyCode::PageUp => {
                    self.scroll += 10;
                }
                KeyCode::PageDown => {
                    self.scroll = self.scroll.saturating_sub(10);
                }
                _ => {}
            },
        }
    }

    fn send(&mut self) {
        let text = std::mem::take(&mut self.input);
        self.cursor = 0;
        self.scroll = 0; // auto-scroll on send

        if text.is_empty() {
            return;
        }

        if let Some(cmd) = text.strip_prefix('/') {
            self.dispatch_command(cmd);
            return;
        }

        if let Some(h) = &self.handle {
            let h = h.clone();
            let t = text.clone();
            tokio::spawn(async move {
                let _ = h.send(&t).await;
            });
        }
    }

    fn dispatch_command(&mut self, cmd: &str) {
        let h = match &self.handle {
            Some(h) => h.clone(),
            None => {
                self.push("system".into(), "room not ready".into(), true);
                return;
            }
        };

        let tx = match &self.cmd_tx {
            Some(tx) => tx.clone(),
            None => {
                self.push("system".into(), "command channel not ready".into(), true);
                return;
            }
        };

        match cmd {
            "invite" => {
                tokio::spawn(async move {
                    let code = h.invite().await.map_err(|e| e.to_string());
                    let _ = tx.send(CmdResult::Invite { code });
                });
            }
            "peers" => {
                tokio::spawn(async move {
                    let peers = h.peers().await;
                    let _ = tx.send(CmdResult::Peers { peers });
                });
            }
            "quit" => {
                tokio::spawn(async move {
                    h.quit().await;
                    let _ = tx.send(CmdResult::Quit);
                });
            }
            _ => {
                self.push("system".into(), format!("unknown command: /{}", cmd), true);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn render(frame: &mut Frame, app: &App) {
    let area = frame.area();

    // Graceful degradation: need at least 4 rows for the layout
    if area.width < 10 || area.height < 4 {
        let msg = format!(
            "Terminal too small: {}x{} (need at least 10x4)",
            area.width, area.height
        );
        let p = Paragraph::new(msg).style(Style::default().fg(Color::Yellow));
        frame.render_widget(p, area);
        return;
    }

    // Layout: top bar (1) | messages (flex) | status (1) | input (1)
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);

    // Top bar
    let top_text = if let Some(addr) = &app.onion {
        let truncated = if addr.len() > 12 { &addr[..12] } else { addr };
        format!("{} [{}...]", APP_NAME, truncated)
    } else {
        APP_NAME.to_string()
    };
    let top = Paragraph::new(Span::styled(
        top_text,
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(top, chunks[0]);

    match &app.mode {
        Mode::Bootstrap { progress, msg } => {
            render_bootstrap(frame, app, chunks[1], *progress, msg);
        }
        Mode::Running | Mode::ShuttingDown { .. } => {
            render_messages(frame, app, chunks[1]);
        }
    }

    // Status bar
    render_status(frame, app, chunks[2]);

    // Input bar
    render_input(frame, app, chunks[3]);
}

fn render_bootstrap(
    frame: &mut Frame,
    app: &App,
    area: ratatui::layout::Rect,
    progress: u8,
    msg: &str,
) {
    let elapsed = app.bootstrap_start.elapsed().as_secs();
    let frame_idx = (elapsed as usize) % SPINNER.len();
    let spinner = SPINNER[frame_idx];

    let line = if progress == 0 {
        format!("  {}  {}", spinner, msg)
    } else if progress < 100 {
        format!("  {}  {}  {}%", spinner, msg, progress)
    } else {
        format!("  {}  connecting...", spinner)
    };

    let p = Paragraph::new(line).block(Block::default().style(Style::default().fg(Color::Yellow)));
    frame.render_widget(p, area);
}

fn render_messages(frame: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let max_visible = area.height as usize;
    let total = app.msgs.len();

    // Calculate visible window
    let visible_end = total.saturating_sub(app.scroll);
    let visible_start = visible_end.saturating_sub(max_visible);

    let lines: Vec<Line> = app.msgs[visible_start..visible_end]
        .iter()
        .map(|m| {
            let prefix = if m.system {
                Span::styled(
                    "[system] ",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::DIM),
                )
            } else {
                let truncated = if m.name.len() > MAX_DISPLAY_NAME {
                    format!("{}...", &m.name[..MAX_DISPLAY_NAME.saturating_sub(3)])
                } else {
                    m.name.clone()
                };
                Span::styled(
                    format!("[{}] ", truncated),
                    Style::default()
                        .fg(Color::Green)
                        .add_modifier(Modifier::BOLD),
                )
            };

            let ts_span = m.ts.map(|ts: chrono::DateTime<chrono::Local>| {
                Span::styled(
                    format!("{} ", ts.format("%H:%M")),
                    Style::default().fg(Color::DarkGray),
                )
            });

            let text_span = Span::raw(&m.text);

            let mut spans = Vec::new();
            if let Some(ts) = ts_span {
                spans.push(ts);
            }
            spans.push(prefix);
            spans.push(text_span);
            Line::from(spans)
        })
        .collect();

    let p = Paragraph::new(lines);
    frame.render_widget(p, area);

    // Scroll indicator
    if !app.at_bottom() {
        let indicator = Span::styled(
            " ▼ more below",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
        let indicator_area = ratatui::layout::Rect {
            x: area.x,
            y: area.bottom().saturating_sub(1),
            width: area.width.min(15),
            height: 1,
        };
        frame.render_widget(Paragraph::new(indicator), indicator_area);
    }
}

fn render_status(frame: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let text = if app.peers.is_empty() {
        "no peers".to_string()
    } else {
        let names: Vec<_> = app.peers.iter().map(|p| p.name.as_str()).collect();
        format!("peers: {}", names.join(", "))
    };

    let style = match &app.mode {
        Mode::ShuttingDown { .. } => Style::default().fg(Color::Red),
        _ => Style::default().fg(Color::Gray),
    };

    let p = Paragraph::new(Span::styled(text, style));
    frame.render_widget(p, area);
}

fn render_input(frame: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let prompt = "> ";
    let text = format!("{}{}", prompt, app.input);

    let style = match (&app.mode, app.input_focused) {
        (Mode::Running, true) => Style::default().fg(Color::White),
        (Mode::Running, false) => Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM),
        _ => Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM),
    };

    let p = Paragraph::new(Span::styled(text, style));
    frame.render_widget(p, area);
}

// ---------------------------------------------------------------------------
// Terminal helpers
// ---------------------------------------------------------------------------

fn setup_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).map_err(Into::into)
}

fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen);
}

fn install_panic_hook() {
    let orig = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal();
        orig(info);
    }));
}

// ---------------------------------------------------------------------------
// Name resolution (same as before)
// ---------------------------------------------------------------------------

fn resolve_name(override_name: Option<String>) -> io::Result<String> {
    if let Some(name) = override_name {
        let trimmed = name.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }

    let config_dir = config_dir();
    let name_path = config_dir.join(NAME_FILE);

    if let Ok(contents) = fs::read_to_string(&name_path) {
        let trimmed = contents.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }

    print!("Enter display name: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    let name = input.trim().to_string();
    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Display name cannot be empty",
        ));
    }

    if let Err(e) = fs::create_dir_all(&config_dir) {
        eprintln!("Warning: could not create config directory: {e}");
    } else if let Err(e) = fs::write(&name_path, &name) {
        eprintln!(
            "Warning: could not save name to {}: {e}",
            name_path.display()
        );
    }

    Ok(name)
}

fn config_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
        .join(CONFIG_DIR_NAME)
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "chat")]
#[command(about = "Ephemeral peer-to-peer chat over Tor", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Host a new chat room
    Host {
        #[arg(long, default_value_t = DEFAULT_INVITE_TTL, value_name = "SECONDS")]
        invite_ttl: u64,

        #[arg(long)]
        name: Option<String>,

        #[arg(long, default_value_t = false)]
        timestamps: bool,
    },
    /// Join an existing chat room
    Join {
        invite_code: String,

        #[arg(long)]
        name: Option<String>,

        #[arg(long, default_value_t = false)]
        timestamps: bool,
    },
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let Some(command) = cli.command else {
        eprintln!("Error: no subcommand provided.\n");
        eprintln!("Usage: chat <COMMAND>");
        eprintln!("\nCommands:");
        eprintln!("  host    Host a new chat room");
        eprintln!("  join    Join an existing chat room");
        eprintln!("\nRun 'chat --help' for more information.");
        std::process::exit(1);
    };

    let (name, timestamps) = match &command {
        Commands::Host {
            name, timestamps, ..
        } => {
            let n = resolve_name(name.clone())
                .map_err(|e| anyhow::anyhow!("Failed to resolve display name: {e}"))?;
            (n, *timestamps)
        }
        Commands::Join {
            name, timestamps, ..
        } => {
            let n = resolve_name(name.clone())
                .map_err(|e| anyhow::anyhow!("Failed to resolve display name: {e}"))?;
            (n, *timestamps)
        }
    };

    // Setup terminal
    install_panic_hook();
    let mut terminal = setup_terminal()?;

    // Start room
    let (handle, mut event_rx) = match &command {
        Commands::Host { invite_ttl, .. } => {
            let config = HostConfig {
                name: name.clone(),
                invite_ttl_secs: *invite_ttl,
            };
            host(config)
        }
        Commands::Join { invite_code, .. } => {
            let config = JoinConfig {
                name: name.clone(),
                invite_code: invite_code.clone(),
            };
            join(config)
        }
    };

    // App state
    let mut app = App::new(name, timestamps);
    app.handle = Some(handle);

    // Input channel from spawn_blocking task
    let (key_tx, mut key_rx) = mpsc::unbounded_channel::<KeyEvent>();

    // Spawn input polling on a blocking thread
    tokio::task::spawn_blocking(move || {
        loop {
            match event::poll(Duration::from_millis(TICK_MS)) {
                Ok(true) => {
                    if let Ok(Event::Key(key)) = event::read() {
                        let _ = key_tx.send(key);
                    }
                }
                Ok(false) => {
                    // timeout — tick
                }
                Err(_) => break,
            }
        }
    });

    // Command result channel (for async /invite, /peers, /quit)
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<CmdResult>();
    app.set_cmd_tx(cmd_tx);

    // Tick timer for spinner animation during bootstrap
    let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));

    // Shutdown deadline
    let mut shutdown_deadline: Option<Instant> = None;

    // Main loop
    loop {
        tokio::select! {
            // Key input
            Some(key) = key_rx.recv() => {
                app.handle_key(key);
            }

            // Chat events
            maybe_ev = event_rx.recv() => {
                match maybe_ev {
                    Some(ev) => app.handle_event(ev),
                    None => {
                        // Event stream ended — room shut down
                        if !matches!(app.mode, Mode::ShuttingDown { .. }) {
                            app.push("system".into(), "connection lost".into(), true);
                            app.mode = Mode::ShuttingDown { since: Instant::now() };
                        }
                    }
                }
            }

            // Command results (async /invite, /peers, /quit)
            Some(result) = cmd_rx.recv() => {
                match result {
                    CmdResult::Invite { code } => {
                        match code {
                            Ok(c) => app.push("system".into(), format!("invite code: {}", c), true),
                            Err(e) => app.push("system".into(), format!("invite failed: {}", e), true),
                        }
                    }
                    CmdResult::Peers { peers } => {
                        if peers.is_empty() {
                            app.push("system".into(), "no peers connected".into(), true);
                        } else {
                            let names: Vec<_> = peers.iter().map(|p| p.name.as_str()).collect();
                            app.push("system".into(), format!("peers: {}", names.join(", ")), true);
                        }
                    }
                    CmdResult::Quit => {
                        app.quit = true;
                    }
                }
            }

            // Timer tick (for spinner redraw)
            _ = ticker.tick() => {}
        }

        // Check shutdown deadline (5 seconds max)
        if let Mode::ShuttingDown { since } = app.mode {
            if since.elapsed() > Duration::from_secs(5) {
                app.quit = true;
            }
        }

        // Draw
        if let Err(e) = terminal.draw(|f| render(f, &app)) {
            eprintln!("render error: {e}");
            app.quit = true;
        }

        // Check quit
        if app.quit {
            break;
        }

        // Auto-set shutdown deadline when entering ShuttingDown
        if matches!(app.mode, Mode::ShuttingDown { .. }) && shutdown_deadline.is_none() {
            shutdown_deadline = Some(Instant::now() + Duration::from_secs(5));
        }

        // Force quit after deadline
        if let Some(dl) = shutdown_deadline {
            if Instant::now() >= dl {
                break;
            }
        }
    }

    // Graceful shutdown
    if let Some(h) = app.handle.take() {
        let _ = tokio::time::timeout(Duration::from_secs(2), async { h.quit().await }).await;
    }

    // Restore terminal
    restore_terminal();

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use ephemeral_chat_core::HostConfig;

    async fn make_app() -> (App, RoomHandle, mpsc::UnboundedReceiver<CmdResult>) {
        let mut app = App::new("tester".into(), false);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<CmdResult>();
        app.set_cmd_tx(cmd_tx);
        let (handle, mut ev_rx) = ephemeral_chat_core::host(HostConfig {
            name: "tester".into(),
            invite_ttl_secs: 300,
        });
        // Feed RoomReady into App so mode becomes Running
        while let Some(ev) = ev_rx.recv().await {
            app.handle_event(ev.clone());
            if matches!(ev, ephemeral_chat_core::ChatEvent::RoomReady { .. }) {
                break;
            }
        }
        app.handle = Some(handle.clone());
        (app, handle, cmd_rx)
    }

    #[tokio::test]
    async fn typing_and_sending_does_not_quit() {
        let (mut app, handle, mut cmd_rx) = make_app().await;
        assert!(!app.quit);

        // Simulate typing "hello"
        for c in "hello".chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        assert_eq!(app.input, "hello");
        assert!(!app.quit, "typing must not quit");

        // Simulate Enter → send
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.input.is_empty(), "input should be consumed");
        assert!(!app.quit, "sending must not quit");

        // Give spawned task time to execute
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Drain any cmd results
        while cmd_rx.try_recv().is_ok() {}

        assert!(!app.quit, "after send task completes, must still be alive");

        // THIS IS THE BUG: second send fails because host_task exited
        let r = handle.send("still alive").await;
        assert!(r.is_ok(), "second send FAILED — host_task exited after first send with no peers. Result: {:?}", r);

        handle.quit().await;
    }

    #[tokio::test]
    async fn typing_individual_chars_does_not_quit() {
        let (mut app, handle, _) = make_app().await;
        assert!(!app.quit);

        let chars = vec![
            ('a', KeyModifiers::NONE),
            ('b', KeyModifiers::NONE),
            (' ', KeyModifiers::NONE),
            ('Z', KeyModifiers::SHIFT),
            ('1', KeyModifiers::NONE),
        ];
        for (c, modif) in chars {
            app.handle_key(KeyEvent::new(KeyCode::Char(c), modif));
            assert!(!app.quit, "char '{}' with {:?} must not quit", c, modif);
        }
        assert_eq!(app.input, "ab Z1");

        handle.quit().await;
    }

    #[test]
    fn ctrl_c_sets_quit() {
        // Can't use make_app() here — it's async and needs tokio.
        // Just test the handle_key logic directly.
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<CmdResult>();
        let mut app = App::new("tester".into(), false);
        app.set_cmd_tx(cmd_tx);
        assert!(!app.quit);
        app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert!(app.quit);
    }
}
