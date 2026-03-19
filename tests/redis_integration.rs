use std::env;
use std::time::Duration;

use reditop::cluster::discover_cluster_targets;
use reditop::model::{
    BigkeysScanStatus, RuntimeSettings, SortMode, Target, TargetProtocol, UiTheme, ViewMode,
};
use reditop::poller::{PollerRequest, start};
use tokio::sync::mpsc;
use tokio::time::timeout;

fn runtime_settings() -> RuntimeSettings {
    RuntimeSettings {
        refresh_interval: Duration::from_secs(60),
        connect_timeout: Duration::from_millis(300),
        command_timeout: Duration::from_secs(2),
        concurrency_limit: 2,
        default_view: ViewMode::Flat,
        default_sort: SortMode::Address,
        ui_theme: UiTheme::default(),
    }
}

fn standalone_target() -> Target {
    Target {
        alias: Some("standalone".to_string()),
        addr: env::var("REDITOP_TEST_REDIS_ADDR").unwrap_or_else(|_| "localhost:6379".to_string()),
        protocol: TargetProtocol::Tcp,
        username: None,
        password: None,
        tags: Vec::new(),
    }
}

fn cluster_target() -> Target {
    Target {
        alias: Some("cluster".to_string()),
        addr: env::var("REDITOP_TEST_REDIS_CLUSTER_ADDR")
            .unwrap_or_else(|_| "localhost:7000".to_string()),
        protocol: TargetProtocol::Tcp,
        username: None,
        password: None,
        tags: Vec::new(),
    }
}

async fn recv_state(
    update_rx: &mut mpsc::Receiver<reditop::model::InstanceState>,
    key: &str,
) -> reditop::model::InstanceState {
    loop {
        let state = timeout(Duration::from_secs(5), update_rx.recv())
            .await
            .expect("timed out waiting for poller update")
            .expect("poller update channel closed unexpectedly");
        if state.key == key {
            return state;
        }
    }
}

fn skip_unreachable(label: &str, err: &str) {
    eprintln!("skipping {label} integration test: {err}");
}

#[tokio::test]
async fn standalone_poll_and_bigkeys_scan_work_against_live_redis() {
    let target = standalone_target();
    let settings = runtime_settings();
    let (mut update_rx, request_tx) = start(vec![target.clone()], settings);

    let state = recv_state(&mut update_rx, &target.addr).await;
    if state.status != reditop::model::Status::Ok {
        skip_unreachable(
            "standalone redis",
            state.last_error.as_deref().unwrap_or("poll failed"),
        );
        return;
    }

    request_tx
        .send(PollerRequest::RefreshBigkeys {
            key: target.addr.clone(),
            force: true,
        })
        .await
        .expect("failed to request bigkeys refresh");

    let running = recv_state(&mut update_rx, &target.addr).await;
    assert_eq!(running.detail.bigkeys.status, BigkeysScanStatus::Running);

    let scanned = recv_state(&mut update_rx, &target.addr).await;
    assert_eq!(scanned.detail.bigkeys.status, BigkeysScanStatus::Ready);
    assert!(scanned.detail.bigkeys.last_error.is_none());
}

#[tokio::test]
async fn cluster_discovery_and_bigkeys_scan_work_against_live_cluster() {
    let seed = cluster_target();
    let settings = runtime_settings();

    let discovered = match discover_cluster_targets(std::slice::from_ref(&seed), &settings).await {
        Ok(discovered) => discovered,
        Err(err) => {
            skip_unreachable("redis cluster discovery", &err.to_string());
            return;
        }
    };
    assert!(!discovered.is_empty());

    let (mut update_rx, request_tx) = start(vec![seed.clone()], settings);
    let state = recv_state(&mut update_rx, &seed.addr).await;
    if state.status != reditop::model::Status::Ok {
        skip_unreachable(
            "redis cluster poll",
            state.last_error.as_deref().unwrap_or("poll failed"),
        );
        return;
    }

    request_tx
        .send(PollerRequest::RefreshBigkeys {
            key: seed.addr.clone(),
            force: true,
        })
        .await
        .expect("failed to request cluster bigkeys refresh");

    let running = recv_state(&mut update_rx, &seed.addr).await;
    assert_eq!(running.detail.bigkeys.status, BigkeysScanStatus::Running);

    let scanned = recv_state(&mut update_rx, &seed.addr).await;
    assert_eq!(
        scanned.detail.bigkeys.status,
        BigkeysScanStatus::Ready,
        "cluster bigkeys scan failed: {:?}",
        scanned.detail.bigkeys.last_error
    );
    assert!(scanned.detail.bigkeys.last_error.is_none());
}
