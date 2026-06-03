use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, Subcommand};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ephemeral_chat_core::factory::{RoomConfig, RoomFactory};
use ephemeral_chat_core::{ChatEvent, PeerInfo, RoomHandle};
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
const ONION_DISPLAY_LEN: usize = 12;

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
                self.push(
                    "system".into(),
                    format!("room ready: {}...", truncate_onion(&onion_address)),
                    true,
                );
            }
            ChatEvent::PeerJoin(info) => {
                if !self.peers.iter().any(|p| p.id == info.id) {
                    self.peers.push(info.clone());
                }
                self.push("system".into(), format!("{} joined", info.name), true);
                if matches!(self.mode, Mode::Bootstrap { .. }) {
                    self.mode = Mode::Running;
                }
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
            self.dispatch_command(&text[1..]);
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

        let parts: Vec<&str> = cmd.split_whitespace().collect();
        let command_name = parts.first().copied().unwrap_or("");

        match command_name {
            "invite" => {
                let invite_cmd = cmd.to_string();
                tokio::spawn(async move {
                    let parts: Vec<&str> = invite_cmd.split_whitespace().collect();
                    let name_arg = if parts.len() > 1 { Some(parts[1]) } else { None };
                    let code = h.invite(name_arg).await.map_err(|e| e.to_string());
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
            "help" => {
                self.push("system".into(), "available commands:".into(), true);
                self.push("system".into(), "  /invite [name]  — generate an invite code".into(), true);
                self.push("system".into(), "  /peers         — list connected peers".into(), true);
                self.push("system".into(), "  /help          — show this help".into(), true);
                self.push("system".into(), "  /quit          — leave the room".into(), true);
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
        format!("{} [{}...]", APP_NAME, truncate_onion(addr))
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

fn truncate_onion(addr: &str) -> &str {
    if addr.len() > ONION_DISPLAY_LEN {
        &addr[..ONION_DISPLAY_LEN]
    } else {
        addr
    }
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

fn create_room(name: &str, command: &Commands) -> (RoomHandle, mpsc::Receiver<ChatEvent>) {
    let config = match command {
        Commands::Host { invite_ttl, .. } => RoomConfig::Host {
            name: name.into(),
            invite_ttl_secs: *invite_ttl,
        },
        Commands::Join { invite_code, .. } => RoomConfig::Join {
            name: name.into(),
            invite_code: invite_code.clone(),
        },
    };
    RoomFactory::create(config)
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

        /// Run without TUI, reading input from stdin
        #[arg(long, default_value_t = false)]
        headless: bool,
    },
    /// Join an existing chat room
    Join {
        invite_code: String,

        #[arg(long)]
        name: Option<String>,

        #[arg(long, default_value_t = false)]
        timestamps: bool,

        /// Run without TUI, reading input from stdin
        #[arg(long, default_value_t = false)]
        headless: bool,
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

    let name_override = match &command {
        Commands::Host { name, .. } | Commands::Join { name, .. } => name.clone(),
    };
    let timestamps = match &command {
        Commands::Host { timestamps, .. } | Commands::Join { timestamps, .. } => *timestamps,
    };
    let headless = match &command {
        Commands::Host { headless, .. } | Commands::Join { headless, .. } => *headless,
    };

    // For join, only use explicit --name flag; fall back to invite's suggested name
    let name = match &command {
        Commands::Join { .. } => {
            if let Some(n) = name_override {
                n.trim().to_string()
            } else {
                String::new() // Empty means "use invite's suggested name"
            }
        }
        Commands::Host { .. } => {
            resolve_name(name_override)
                .map_err(|e| anyhow::anyhow!("Failed to resolve display name: {e}"))?
        }
    };

    if headless {
        run_headless(name, timestamps, &command).await
    } else {
        run_tui(name, timestamps, &command).await
    }
}

// ---------------------------------------------------------------------------
// TUI mode
// ---------------------------------------------------------------------------

async fn run_tui(name: String, timestamps: bool, command: &Commands) -> Result<()> {
    install_panic_hook();
    let mut terminal = setup_terminal()?;

    let (handle, mut event_rx) = create_room(&name, command);

    let mut app = App::new(name, timestamps);
    app.handle = Some(handle);

    let (key_tx, mut key_rx) = mpsc::unbounded_channel::<KeyEvent>();

    tokio::task::spawn_blocking(move || {
        loop {
            if event::poll(Duration::from_millis(TICK_MS)).unwrap_or(false) {
                if let Ok(Event::Key(key)) = event::read() {
                    let _ = key_tx.send(key);
                }
            }
        }
    });

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<CmdResult>();
    app.set_cmd_tx(cmd_tx);

    let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));

    loop {
        tokio::select! {
            Some(key) = key_rx.recv() => {
                app.handle_key(key);
            }

            maybe_ev = event_rx.recv() => {
                if let Some(ev) = maybe_ev {
                    app.handle_event(ev);
                } else if !matches!(app.mode, Mode::ShuttingDown { .. }) {
                    app.push("system".into(), "connection lost".into(), true);
                    app.mode = Mode::ShuttingDown { since: Instant::now() };
                }
            }

            Some(result) = cmd_rx.recv() => {
                match result {
                    CmdResult::Invite { code } => match code {
                        Ok(c) => app.push("system".into(), format!("invite code: {}", c), true),
                        Err(e) => app.push("system".into(), format!("invite failed: {}", e), true),
                    },
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

            _ = ticker.tick() => {}
        }

        if let Mode::ShuttingDown { since } = app.mode {
            if since.elapsed() > Duration::from_secs(5) {
                app.quit = true;
            }
        }

        if let Err(e) = terminal.draw(|f| render(f, &app)) {
            eprintln!("render error: {e}");
            app.quit = true;
        }

        if app.quit {
            break;
        }
    }

    if let Some(h) = app.handle.take() {
        let _ = tokio::time::timeout(Duration::from_secs(2), async { h.quit().await }).await;
    }

    restore_terminal();
    Ok(())
}

// ---------------------------------------------------------------------------
// Headless mode (stdin/stdout text interface)
// ---------------------------------------------------------------------------

async fn run_headless(name: String, _timestamps: bool, command: &Commands) -> Result<()> {
    let (handle, mut event_rx) = create_room(&name, command);
    let mut room_ready = false;

    let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
    std::thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            let Ok(l) = line else { break };
            if line_tx.send(l).is_err() { break; }
        }
    });

    loop {
        tokio::select! {
            Some(line) = line_rx.recv() => {
                let line = line.trim().to_string();
                if line.is_empty() { continue; }
                if line == "/quit" { break; }

                if let Some(cmd) = line.strip_prefix('/') {
                    let h = handle.clone();
                    let parts: Vec<&str> = cmd.split_whitespace().collect();
                    let command_name = parts.first().copied().unwrap_or("");
                    match command_name {
                        "invite" => {
                            let invite_cmd = cmd.to_string();
                            tokio::spawn(async move {
                                let parts: Vec<&str> = invite_cmd.split_whitespace().collect();
                                let name_arg = if parts.len() > 1 { Some(parts[1]) } else { None };
                                match h.invite(name_arg).await {
                                    Ok(c) => println!("[system] invite code: {}", c),
                                    Err(e) => eprintln!("[error] invite failed: {}", e),
                                }
                            });
                        }
                        "peers" => {
                            let h = handle.clone();
                            tokio::spawn(async move {
                                let peers = h.peers().await;
                                if peers.is_empty() {
                                    println!("[system] no peers");
                                } else {
                                    let names: Vec<_> = peers.iter().map(|p| p.name.as_str()).collect();
                                    println!("[system] peers: {}", names.join(", "));
                                }
                            });
                        }
                        _ => println!("[system] unknown command: /{}", cmd),
                    }
                } else if room_ready {
                    let h = handle.clone();
                    tokio::spawn(async move {
                        let _ = h.send(&line).await;
                    });
                }
            }
            maybe_ev = event_rx.recv() => {
                let Some(ev) = maybe_ev else {
                    eprintln!("\n[system] connection lost");
                    break;
                };
                match ev {
                    ChatEvent::BootstrapProgress(pct) => {
                        if pct > 0 && pct < 100 {
                            eprint!("\rBootstrapping: {}%  ", pct);
                        }
                    }
                    ChatEvent::RoomReady { onion_address, .. } => {
                        room_ready = true;
                        println!("\n[system] room ready: {}...", truncate_onion(&onion_address));
                    }
                    ChatEvent::PeerJoin(info) => {
                        println!("[system] {} joined", info.name);
                        room_ready = true;
                    }
                    ChatEvent::PeerLeave(pid) => {
                        println!("[system] {} left", pid);
                    }
                    ChatEvent::Message { name, text, .. } => {
                        println!("[{}] {}", name, text);
                    }
                    ChatEvent::InviteCreated { code } => {
                        println!("[system] new invite code: {}", code);
                    }
                    ChatEvent::RoomClosed => {
                        println!("[system] room closed");
                        break;
                    }
                    ChatEvent::Error(e) => {
                        eprintln!("\n[error] {}", e);
                    }
                }
            }
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(2), async { handle.quit().await }).await;
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

    #[test]
    fn typing_and_sending_does_not_quit() {
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<CmdResult>();
        let mut app = App::new("tester".into(), false);
        app.set_cmd_tx(cmd_tx);
        app.mode = Mode::Running;
        assert!(!app.quit);

        for c in "hello".chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE));
        }
        assert_eq!(app.input, "hello");
        assert!(!app.quit, "typing must not quit");

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.input.is_empty(), "input should be consumed");
        assert!(!app.quit, "sending must not quit");
    }

    #[tokio::test]
    #[ignore = "requires real Tor bootstrap (~16s); move to cli_e2e suite if needed"]
    async fn second_send_with_no_peers_works() {
        let (handle, mut ev_rx) = ephemeral_chat_core::host(HostConfig {
            name: "tester".into(),
            invite_ttl_secs: 300,
        });

        let mut room_ready = false;
        while let Some(ev) = ev_rx.recv().await {
            if matches!(ev, ephemeral_chat_core::ChatEvent::RoomReady { .. }) {
                room_ready = true;
                break;
            }
        }
        if !room_ready {
            return; // Tor bootstrap unavailable
        }

        let r = handle.send("hello").await;
        assert!(r.is_ok(), "first send failed");

        let r = handle.send("still alive").await;
        assert!(r.is_ok(), "second send FAILED — host_task exited after first send with no peers");

        handle.quit().await;
    }

    #[test]
    fn typing_individual_chars_does_not_quit() {
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<CmdResult>();
        let mut app = App::new("tester".into(), false);
        app.set_cmd_tx(cmd_tx);
        app.mode = Mode::Running;
        assert!(!app.quit);

        let chars = [
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
