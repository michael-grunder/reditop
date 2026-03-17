#![warn(clippy::all, clippy::nursery, clippy::pedantic)]

mod app;
mod cli;
mod cluster;
mod column;
mod columns;
mod config;
mod model;
mod parse;
mod poller;
mod registry;
mod target_addr;
mod topology;
mod tui;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let launch = cli::build_launch_config().await?;
    tui::run(launch).await
}
