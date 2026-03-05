mod app;
mod cli;
mod config;
mod model;
mod parse;
mod poller;
mod target_addr;
mod topology;
mod tui;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let launch = cli::build_launch_config()?;
    tui::run(launch).await
}
