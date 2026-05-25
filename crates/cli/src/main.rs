use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use ephemeral_chat_core::{host, join, HostConfig, JoinConfig};

const DEFAULT_INVITE_TTL: u64 = 300; // 5 minutes
const CONFIG_DIR_NAME: &str = "ephemeral-chat";
const NAME_FILE: &str = "name";

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
        /// Invite code time-to-live in seconds
        #[arg(long, default_value_t = DEFAULT_INVITE_TTL, value_name = "SECONDS")]
        invite_ttl: u64,

        /// Display name for this session
        #[arg(long)]
        name: Option<String>,

        /// Show timestamps on messages
        #[arg(long, default_value_t = false)]
        timestamps: bool,
    },
    /// Join an existing chat room
    Join {
        /// Invite code to join the room
        invite_code: String,

        /// Display name for this session
        #[arg(long)]
        name: Option<String>,

        /// Show timestamps on messages
        #[arg(long, default_value_t = false)]
        timestamps: bool,
    },
}

/// Resolve the display name: CLI flag > persisted file > prompt user.
fn resolve_name(override_name: Option<String>) -> io::Result<String> {
    // CLI flag takes priority
    if let Some(name) = override_name {
        let trimmed = name.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }

    let config_dir = config_dir();
    let name_path = config_dir.join(NAME_FILE);

    // Try reading persisted name
    if let Ok(contents) = fs::read_to_string(&name_path) {
        let trimmed = contents.trim().to_string();
        if !trimmed.is_empty() {
            return Ok(trimmed);
        }
    }

    // Prompt user for name
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

    // Persist name for future runs
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

/// Get the config directory path (~/.config/ephemeral-chat).
fn config_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
        .join(CONFIG_DIR_NAME)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    match command {
        Commands::Host {
            invite_ttl,
            name,
            timestamps: _,
        } => {
            let resolved_name = resolve_name(name)
                .map_err(|e| anyhow::anyhow!("Failed to resolve display name: {e}"))?;

            println!("Hosting as '{resolved_name}' (invite TTL: {invite_ttl}s)");

            let config = HostConfig {
                name: resolved_name,
                invite_ttl_secs: invite_ttl,
            };

            let (handle, _event_stream) = host(config);

            // Wait for Ctrl-C
            tokio::signal::ctrl_c().await?;
            println!("\nShutting down...");
            handle.quit().await;
        }
        Commands::Join {
            invite_code,
            name,
            timestamps: _,
        } => {
            let resolved_name = resolve_name(name)
                .map_err(|e| anyhow::anyhow!("Failed to resolve display name: {e}"))?;

            println!("Joining as '{resolved_name}'");

            let config = JoinConfig {
                name: resolved_name,
                invite_code,
            };

            let (handle, _event_stream) = join(config);

            // Wait for Ctrl-C
            tokio::signal::ctrl_c().await?;
            println!("\nShutting down...");
            handle.quit().await;
        }
    }

    Ok(())
}
