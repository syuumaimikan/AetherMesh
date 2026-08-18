//! The dashboard against a real controller over a real socket.
//!
//! The unit tests cover the state machine with hand-built replies. These cover
//! the thing those cannot: that the frames this sends are the frames a
//! controller answers, and that what comes back is what the screen expects.

use std::time::{Duration, Instant};

use aether_controller::{
    ClientGateway, Controller, MeshState, SecurityConfig, SimulatedMesh, bind_clients,
    run_dispatcher, serve_clients,
};
use aether_core::{NodeId, NodeInfo};
use aether_scheduler::{DataCatalog, LeastLoadedScheduler};
use aether_tui::app::{App, Key, LineKind, LinkState, Mode};
use aether_tui::{Connection, SubmitOptions};

const TIMEOUT: Duration = Duration::from_secs(5);

/// A controller listening on a real port, with one node registered.
async fn controller(token: Option<&str>) -> (String, MeshState) {
    let state = MeshState::new();
    let controller = Controller::new(
        LeastLoadedScheduler::new(),
        SimulatedMesh::new(),
        DataCatalog::new(),
    )
    .with_traffic_stats(state.traffic.clone());

    let (gateway, commands) = ClientGateway::new(8);
    tokio::spawn(run_dispatcher(controller, state.clone(), commands));

    let security = match token {
        Some(token) => SecurityConfig::with_token(token),
        None => SecurityConfig::open(),
    };
    let (listener, addr) = bind_clients("127.0.0.1:0".parse().unwrap()).await.unwrap();
    tokio::spawn(serve_clients(listener, gateway, security));

    let info = NodeInfo::new(NodeId::generate(), "worker", "127.0.0.1:7001", 4)
        .with_label("kind", "cpu")
        .with_latency_ms(3.5);
    state.registry.lock().unwrap().register(info);

    (addr.to_string(), state)
}

#[tokio::test]
async fn the_dashboard_reads_a_real_controller() {
    let (addr, _state) = controller(None).await;
    let mut client = Connection::connect(&addr, None, TIMEOUT).await.unwrap();
    let mut app = App::new(addr, Duration::from_secs(1));

    app.apply_stats(client.stats().await.unwrap(), Instant::now());
    app.apply_nodes(client.nodes().await.unwrap());

    assert_eq!(app.connection, LinkState::Live);
    assert_eq!(app.totals.nodes, 1);
    let node = app.selected_node().expect("the registered node");
    assert_eq!(node.hostname, "worker");
    assert_eq!(node.latency_ms, Some(3.5));
    assert_eq!(node.labels.get("kind").map(String::as_str), Some("cpu"));
}

#[tokio::test]
async fn submitting_from_the_dashboard_runs_the_task_and_moves_the_counters() {
    let (addr, _state) = controller(None).await;
    let mut client = Connection::connect(&addr, None, TIMEOUT).await.unwrap();
    let mut app = App::new(addr, Duration::from_secs(1));
    app.apply_nodes(client.nodes().await.unwrap());

    // What pressing s, typing, and pressing enter produces.
    app.open_form();
    let submission = app
        .edit_form(Key::Enter)
        .expect("the default form is valid");
    assert_eq!(submission.kind, "echo");
    assert_eq!(app.mode, Mode::Watching);
    assert!(app.submitting);

    let result = client
        .submit(
            &submission.kind,
            submission.payload,
            &SubmitOptions::default()
                .with_constraints(submission.constraints)
                .with_priority(submission.priority),
        )
        .await
        .unwrap();
    app.apply_result(result);

    assert!(!app.submitting);
    let line = app.log.front().expect("a result line");
    assert_eq!(line.kind, LineKind::Good, "{}", line.text);
    assert!(line.text.contains("ran on"), "{}", line.text);
}

#[tokio::test]
async fn a_task_no_node_satisfies_is_reported_rather_than_silently_dropped() {
    let (addr, _state) = controller(None).await;
    let mut client = Connection::connect(&addr, None, TIMEOUT).await.unwrap();
    let mut app = App::new(addr, Duration::from_secs(1));

    app.form.constraints = "kind=gpu".to_string();
    let submission = app.edit_form(Key::Enter).expect("a valid form");

    let outcome = client
        .submit(
            &submission.kind,
            submission.payload,
            &SubmitOptions::default()
                .with_constraints(submission.constraints)
                .with_priority(submission.priority),
        )
        .await;

    // The operator asked for a GPU and there is none. The controller says so,
    // and the dashboard shows it rather than a task that silently vanished.
    let message = match outcome {
        Err(error) => error.to_string(),
        Ok(finished) => panic!("expected a refusal, got {finished:?}"),
    };
    app.submitting = false;
    app.push_log(message, LineKind::Bad);

    let line = app.log.front().expect("a line");
    assert_eq!(line.kind, LineKind::Bad);
    assert!(line.text.contains("no node available"), "{}", line.text);
}

#[tokio::test]
async fn traffic_the_controller_reports_becomes_a_rate_on_screen() {
    let (addr, state) = controller(None).await;
    let mut client = Connection::connect(&addr, None, TIMEOUT).await.unwrap();
    let mut app = App::new(addr, Duration::from_secs(1));

    let start = Instant::now();
    app.apply_stats(client.stats().await.unwrap(), start);

    // The dispatcher writes into these while it moves data; simulate a second
    // of transfer and check the dashboard turns cumulative bytes into a rate.
    state.traffic.record_sent(2_000, 8_000);
    app.apply_stats(
        client.stats().await.unwrap(),
        start + Duration::from_secs(2),
    );

    assert_eq!(
        app.throughput.latest(),
        1_000,
        "2000 bytes over two seconds"
    );
    let traffic = app.traffic.expect("traffic");
    assert_eq!(traffic.bytes_saved_by_compression, 6_000);
}

#[tokio::test]
async fn a_controller_that_wants_a_token_refuses_a_dashboard_without_one() {
    let (addr, _state) = controller(Some("s3cret")).await;

    let refused = Connection::connect(&addr, None, TIMEOUT).await;
    assert!(refused.is_err(), "an open dashboard on a closed mesh");

    let accepted = Connection::connect(&addr, Some("s3cret".to_string()), TIMEOUT).await;
    assert!(accepted.is_ok());
}

#[tokio::test]
async fn a_controller_that_is_not_there_is_an_error_not_a_hang() {
    // Port 1 on loopback refuses immediately on every platform this runs on.
    let outcome = Connection::connect("127.0.0.1:1", None, Duration::from_millis(500)).await;
    assert!(outcome.is_err());
}
