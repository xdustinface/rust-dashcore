///! Real Dash Core node harness for integration testing.
///!
///! This starts a real dashd instance using existing regtest data,
///! providing full protocol support including compact filters and masternode lists.
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

/// Configuration for Dash Core node
pub struct DashCoreConfig {
    /// Path to dashd binary
    pub dashd_path: PathBuf,
    /// Path to existing datadir with blockchain data
    pub datadir: PathBuf,
    /// RPC username (optional)
    pub rpc_user: Option<String>,
    /// RPC password (optional)
    pub rpc_password: Option<String>,
    /// Wallet name to load on startup
    pub wallet: String,
}

impl Default for DashCoreConfig {
    fn default() -> Self {
        // Use environment variables or relative paths
        let dashd_path = std::env::var("DASHD_PATH").map(PathBuf::from).unwrap_or_else(|_| {
            // Try common locations
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(format!("{}/GIT/dash/src/dashd", home))
        });

        let datadir = std::env::var("DASHD_DATADIR").map(PathBuf::from).unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(format!("{}/dashcore-regtest-data", home))
        });

        Self {
            dashd_path,
            datadir,
            rpc_user: None,
            rpc_password: None,
            wallet: "default".to_string(),
        }
    }
}

/// Harness for managing a Dash Core node
pub struct DashCoreNode {
    config: DashCoreConfig,
    process: Option<Child>,
    p2p_port: u16,
    rpc_port: u16,
}

impl DashCoreNode {
    /// Create a new Dash Core node with default configuration
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_config(DashCoreConfig::default())
    }

    /// Create a new Dash Core node with custom configuration
    pub fn with_config(config: DashCoreConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Verify dashd exists
        if !config.dashd_path.exists() {
            return Err(format!("dashd not found at {:?}", config.dashd_path).into());
        }

        // Use fixed ports for regtest
        let p2p_port = 19999; // Standard regtest P2P port
        let rpc_port = 19998; // Standard regtest RPC port

        Ok(Self {
            config,
            process: None,
            p2p_port,
            rpc_port,
        })
    }

    /// Get a reference to the node configuration
    pub fn config(&self) -> &DashCoreConfig {
        &self.config
    }

    /// Start the Dash Core node
    pub async fn start(&mut self) -> Result<SocketAddr, Box<dyn std::error::Error>> {
        tracing::info!("Starting dashd...");
        tracing::info!("  Binary: {:?}", self.config.dashd_path);
        tracing::info!("  Datadir: {:?}", self.config.datadir);
        tracing::info!("  P2P port: {}", self.p2p_port);
        tracing::info!("  RPC port: {}", self.rpc_port);

        // Ensure datadir exists
        std::fs::create_dir_all(&self.config.datadir)?;

        // Build command
        let mut cmd = Command::new(&self.config.dashd_path);
        cmd.arg("-regtest")
            .arg(format!("-datadir={}", self.config.datadir.display()))
            .arg(format!("-port={}", self.p2p_port))
            .arg(format!("-rpcport={}", self.rpc_port))
            .arg("-server=1")
            .arg("-daemon=0") // Run in foreground
            .arg("-fallbackfee=0.00001")
            .arg("-rpcbind=127.0.0.1")
            .arg("-rpcallowip=127.0.0.1")
            .arg("-listen=1")
            .arg("-txindex=0") // Disable for testing to reduce file usage
            .arg("-addressindex=0") // Disable for testing
            .arg("-spentindex=0") // Disable for testing
            .arg("-timestampindex=0") // Disable for testing
            .arg("-blockfilterindex=1") // Enable compact block filter index
            .arg("-peerblockfilters=1") // Serve compact filters to peers
            .arg("-printtoconsole"); // Print logs to console

        // Add RPC credentials if provided
        if let Some(user) = &self.config.rpc_user {
            cmd.arg(format!("-rpcuser={}", user));
        }
        if let Some(pass) = &self.config.rpc_password {
            cmd.arg(format!("-rpcpassword={}", pass));
        }

        // Add wallet to load
        cmd.arg(format!("-wallet={}", self.config.wallet));

        // Build the full command line
        let mut args_vec = vec![
            format!("-regtest"),
            format!("-datadir={}", self.config.datadir.display()),
            format!("-port={}", self.p2p_port),
            format!("-rpcport={}", self.rpc_port),
            "-server=1".to_string(),
            "-daemon=0".to_string(),
            "-fallbackfee=0.00001".to_string(),
            "-rpcbind=127.0.0.1".to_string(),
            "-rpcallowip=127.0.0.1".to_string(),
            "-listen=1".to_string(),
            "-txindex=0".to_string(),
            "-addressindex=0".to_string(),
            "-spentindex=0".to_string(),
            "-timestampindex=0".to_string(),
            "-blockfilterindex=1".to_string(), // Enable compact block filter index
            "-peerblockfilters=1".to_string(), // Serve compact filters to peers
            "-printtoconsole".to_string(),
        ];

        // Add RPC credentials if provided
        if let Some(user) = &self.config.rpc_user {
            args_vec.push(format!("-rpcuser={}", user));
        }
        if let Some(pass) = &self.config.rpc_password {
            args_vec.push(format!("-rpcpassword={}", pass));
        }

        // Add wallet to load
        args_vec.push(format!("-wallet={}", self.config.wallet));

        // Try running through bash with explicit ulimit
        // Use launchctl to set file descriptor limit if on macOS
        let script = if cfg!(target_os = "macos") {
            format!(
                "launchctl limit maxfiles 10000 unlimited 2>/dev/null || true; ulimit -Sn 10000 2>/dev/null || ulimit -n 10000; exec {} {}",
                self.config.dashd_path.display(),
                args_vec.join(" ")
            )
        } else {
            format!(
                "ulimit -n 10000; exec {} {}",
                self.config.dashd_path.display(),
                args_vec.join(" ")
            )
        };

        let mut child = Command::new("bash")
            .arg("-c")
            .arg(&script)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        // Spawn task to read stderr for debugging
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut reader = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = reader.next_line().await {
                    tracing::debug!("dashd stderr: {}", line);
                }
            });
        }

        self.process = Some(child);

        // Wait for node to be ready by checking if port is open
        tracing::info!("Waiting for dashd to be ready...");

        // First check if process died immediately (e.g., due to lock)
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Some(ref mut proc) = self.process {
            if let Ok(Some(status)) = proc.try_wait() {
                return Err(format!("dashd exited immediately with status: {}", status).into());
            }
        }

        let ready = self.wait_for_ready().await?;
        if !ready {
            // Try to get exit status if process died
            if let Some(ref mut proc) = self.process {
                if let Ok(Some(status)) = proc.try_wait() {
                    return Err(format!("dashd exited with status: {}", status).into());
                }
            }
            return Err("dashd failed to start within timeout".into());
        }

        // Double-check process is still alive after port check
        if let Some(ref mut proc) = self.process {
            if let Ok(Some(status)) = proc.try_wait() {
                return Err(
                    format!("dashd died after port became ready, status: {}", status).into()
                );
            }
        }

        // Give dashd more time to fully initialize P2P layer
        // Dashd v23 needs more time to be ready for P2P connections
        tracing::debug!("Port is open, waiting for P2P layer to fully initialize...");
        tokio::time::sleep(Duration::from_millis(5000)).await;

        let addr = SocketAddr::from(([127, 0, 0, 1], self.p2p_port));
        tracing::info!("✅ dashd started and ready at {}", addr);

        Ok(addr)
    }

    /// Wait for dashd to be ready by checking if P2P port is accepting connections
    async fn wait_for_ready(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let max_wait = Duration::from_secs(30);
        let check_interval = Duration::from_millis(500);

        let result = timeout(max_wait, async {
            loop {
                // Try to connect to P2P port
                let addr = SocketAddr::from((Ipv4Addr::new(127, 0, 0, 1), self.p2p_port));
                if tokio::net::TcpStream::connect(addr).await.is_ok() {
                    tracing::debug!("P2P port is accepting connections");
                    return true;
                }

                sleep(check_interval).await;
            }
        })
        .await;

        Ok(result.unwrap_or(false))
    }

    /// Stop the Dash Core node
    pub async fn stop(&mut self) {
        if let Some(mut process) = self.process.take() {
            tracing::info!("Stopping dashd...");

            // Try graceful shutdown via RPC if possible
            // For now, just kill the process
            let _ = process.kill();
            let _ = process.wait();

            tracing::info!("✅ dashd stopped");
        }
    }

    /// Get the P2P address
    pub fn p2p_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.p2p_port))
    }

    /// Get the RPC port
    pub fn rpc_port(&self) -> u16 {
        self.rpc_port
    }

    /// Get block count via RPC
    pub async fn get_block_count(&self) -> Result<u32, Box<dyn std::error::Error>> {
        // This would use RPC to get block count
        // For now, we'll use dash-cli
        let dash_cli = self
            .config
            .dashd_path
            .parent()
            .and_then(|p| Some(p.join("dash-cli")))
            .ok_or("Could not find dash-cli")?;

        let output = std::process::Command::new(dash_cli)
            .arg("-regtest")
            .arg(format!("-datadir={}", self.config.datadir.display()))
            .arg(format!("-rpcport={}", self.rpc_port))
            .arg("getblockcount")
            .output()?;

        if !output.status.success() {
            return Err(
                format!("dash-cli failed: {}", String::from_utf8_lossy(&output.stderr)).into()
            );
        }

        let count_str = String::from_utf8(output.stdout)?;
        let count_str = count_str.trim();
        if count_str.is_empty() {
            return Err("Empty response from getblockcount".into());
        }
        let count = count_str.parse::<u32>()?;
        Ok(count)
    }
}

impl Drop for DashCoreNode {
    fn drop(&mut self) {
        if let Some(mut process) = self.process.take() {
            tracing::info!("Stopping dashd process in Drop...");

            // Kill the process - this should be sufficient for test cleanup
            if let Err(e) = process.start_kill() {
                tracing::warn!("Failed to kill dashd process: {}", e);
            } else {
                // Give it a moment to clean up
                std::thread::sleep(std::time::Duration::from_millis(500));
                tracing::info!("✅ dashd process stopped");
            }
        }
    }
}

/// Check if dashd is available at the default path
pub fn is_dashd_available() -> bool {
    DashCoreConfig::default().dashd_path.exists()
}

/// Get default dashd path
pub fn default_dashd_path() -> PathBuf {
    DashCoreConfig::default().dashd_path
}
