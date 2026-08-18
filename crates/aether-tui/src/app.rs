//! What the dashboard knows and how a keystroke changes it.
//!
//! Everything here is plain data and pure transitions, so the parts worth being
//! sure about — deriving a rate from cumulative counters, keeping a selection
//! valid while the mesh changes under it — are testable without a terminal.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use aether_controller::client::{NodeSummary, TrafficSummary};
use aether_controller::connection::{Finished, Stats};
use aether_controller::observability::{MetricsSnapshot, QueueSnapshot};
use aether_core::Priority;

/// Samples kept for the throughput graph.
pub const HISTORY: usize = 120;

/// Lines kept in the activity log.
const LOG_LINES: usize = 64;

/// How far the poll interval can be pushed in either direction.
const MIN_POLL: Duration = Duration::from_millis(250);
const MAX_POLL: Duration = Duration::from_secs(10);

/// Whether the controller is answering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkState {
    Connecting,
    Live,
    /// Lost, with the reason. The dashboard keeps showing the last good numbers
    /// rather than blanking: "the mesh went quiet" and "the mesh went empty"
    /// are different facts and must not look the same.
    Lost(String),
}

/// Live counts that are not counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub nodes: usize,
    pub nodes_connected: usize,
    pub datasets: usize,
    pub dataset_bytes: u64,
}

/// What the dashboard is doing right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Watching,
    Submitting,
    Help,
}

/// Which field of the submit form has focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Kind,
    Payload,
    Constraints,
    Priority,
}

impl Field {
    fn next(self) -> Self {
        match self {
            Self::Kind => Self::Payload,
            Self::Payload => Self::Constraints,
            Self::Constraints => Self::Priority,
            Self::Priority => Self::Kind,
        }
    }
}

/// The task the operator is composing.
#[derive(Debug, Clone)]
pub struct Form {
    pub kind: String,
    pub payload: String,
    pub constraints: String,
    /// Cycled with left/right rather than typed: there are five of them and
    /// spelling one wrong should not be possible.
    pub priority: Priority,
    pub focus: Field,
}

impl Default for Form {
    fn default() -> Self {
        Self {
            kind: "echo".to_string(),
            payload: "hello".to_string(),
            constraints: String::new(),
            priority: Priority::Normal,
            focus: Field::Kind,
        }
    }
}

impl Form {
    fn field_mut(&mut self) -> &mut String {
        match self.focus {
            Field::Kind => &mut self.kind,
            Field::Payload => &mut self.payload,
            Field::Constraints => &mut self.constraints,
            // Not a text field; left/right move it instead.
            Field::Priority => &mut self.constraints,
        }
    }

    /// Moves the priority one level, staying inside the range.
    pub fn shift_priority(&mut self, up: bool) {
        let levels = Priority::ALL;
        let index = levels
            .iter()
            .position(|level| *level == self.priority)
            .unwrap_or(2);
        let next = if up {
            (index + 1).min(levels.len() - 1)
        } else {
            index.saturating_sub(1)
        };
        self.priority = levels[next];
    }

    /// The payload bytes this form describes.
    ///
    /// `cpu` takes a little-endian `u64` iteration count, not the digits of
    /// one. Typing a number and getting "expects an 8 byte iteration count"
    /// back is a papercut the dashboard can simply not have.
    pub fn payload_bytes(&self) -> Result<Vec<u8>, String> {
        if self.kind.trim() == "cpu" {
            let text = self.payload.trim();
            let count: u64 = text
                .parse()
                .map_err(|_| format!("cpu takes an iteration count, not {text:?}"))?;
            return Ok(count.to_le_bytes().to_vec());
        }
        Ok(self.payload.clone().into_bytes())
    }

    /// The constraints this form describes: comma or space separated.
    pub fn constraint_list(&self) -> Vec<String> {
        self.constraints
            .split([',', ' '])
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
            .collect()
    }
}

/// One line of the activity log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Line {
    pub text: String,
    pub kind: LineKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineKind {
    Info,
    Good,
    Bad,
}

/// Cumulative counters turned into a rate.
#[derive(Debug, Default)]
pub struct Throughput {
    samples: VecDeque<u64>,
    last_total: Option<u64>,
    last_at: Option<Instant>,
    latest: u64,
}

impl Throughput {
    /// Records a cumulative byte count and derives bytes per second since the
    /// previous one.
    pub fn record(&mut self, total: u64, at: Instant) {
        let rate = match (self.last_total, self.last_at) {
            (Some(previous), Some(then)) => {
                let elapsed = at.saturating_duration_since(then).as_secs_f64();
                // A restarted controller counts from zero again. Saturating,
                // because a negative rate drawn as a huge unsigned one would
                // make the graph lie for the rest of the session.
                let moved = total.saturating_sub(previous);
                if elapsed <= 0.0 {
                    0
                } else {
                    (moved as f64 / elapsed) as u64
                }
            }
            // Nothing to compare the first sample against.
            _ => 0,
        };

        self.last_total = Some(total);
        self.last_at = Some(at);
        self.latest = rate;
        self.samples.push_back(rate);
        while self.samples.len() > HISTORY {
            self.samples.pop_front();
        }
    }

    /// Samples oldest to newest, for the graph.
    pub fn samples(&self) -> Vec<u64> {
        self.samples.iter().copied().collect()
    }

    /// The most recent rate, in bytes per second.
    pub fn latest(&self) -> u64 {
        self.latest
    }

    pub fn peak(&self) -> u64 {
        self.samples.iter().copied().max().unwrap_or(0)
    }
}

/// Everything on screen.
pub struct App {
    pub addr: String,
    pub poll: Duration,
    pub connection: LinkState,
    pub traffic: Option<TrafficSummary>,
    pub mesh: Option<MetricsSnapshot>,
    pub queue: Option<QueueSnapshot>,
    pub totals: Totals,
    pub nodes: Vec<NodeSummary>,
    pub selected: usize,
    pub throughput: Throughput,
    pub log: VecDeque<Line>,
    pub mode: Mode,
    pub form: Form,
    pub quit: bool,
    /// Set while a submission is in flight, so a second one is not queued
    /// before the first has found a node.
    pub submitting: bool,
    /// Set by `r` to poll before the timer is due.
    pub refresh: bool,
}

impl App {
    pub fn new(addr: String, poll: Duration) -> Self {
        Self {
            addr,
            poll: poll.clamp(MIN_POLL, MAX_POLL),
            connection: LinkState::Connecting,
            traffic: None,
            mesh: None,
            queue: None,
            totals: Totals::default(),
            nodes: Vec::new(),
            selected: 0,
            throughput: Throughput::default(),
            log: VecDeque::new(),
            mode: Mode::Watching,
            form: Form::default(),
            quit: false,
            submitting: false,
            refresh: false,
        }
    }

    /// Folds a stats reading into the view.
    pub fn apply_stats(&mut self, stats: Stats, at: Instant) {
        self.throughput.record(stats.traffic.bytes_sent, at);
        self.traffic = Some(stats.traffic);
        self.mesh = Some(stats.mesh);
        self.queue = Some(stats.queue);
        self.totals = Totals {
            nodes: stats.nodes,
            nodes_connected: stats.nodes_connected,
            datasets: stats.datasets,
            dataset_bytes: stats.dataset_bytes,
        };
        self.connection = LinkState::Live;
    }

    /// Folds a node listing into the view, keeping the selection valid.
    pub fn apply_nodes(&mut self, mut nodes: Vec<NodeSummary>) {
        // Stable order, so a node does not jump under the cursor when the
        // controller happens to iterate its map differently.
        nodes.sort_by(|a, b| a.hostname.cmp(&b.hostname).then(a.node_id.cmp(&b.node_id)));
        self.nodes = nodes;
        self.clamp_selection();
        self.connection = LinkState::Live;
    }

    /// Records that the controller stopped answering, keeping the last numbers.
    pub fn lose(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        if self.connection != LinkState::Lost(reason.clone()) {
            self.push_log(format!("controller: {reason}"), LineKind::Bad);
        }
        self.connection = LinkState::Lost(reason);
    }

    /// Turns a task result into a log line.
    pub fn apply_result(&mut self, finished: Finished) {
        self.submitting = false;
        let where_it_ran = short(&finished.node_id).to_string();

        if finished.success {
            self.push_log(
                format!("ran on {where_it_ran} in {:.1} ms", finished.duration_ms),
                LineKind::Good,
            );
            return;
        }

        self.push_log(
            format!(
                "failed on {where_it_ran} after {:.1} ms: {}",
                finished.duration_ms,
                finished
                    .error
                    .unwrap_or_else(|| "no reason given".to_string())
            ),
            LineKind::Bad,
        );
    }

    pub fn push_log(&mut self, text: impl Into<String>, kind: LineKind) {
        self.log.push_front(Line {
            text: text.into(),
            kind,
        });
        while self.log.len() > LOG_LINES {
            self.log.pop_back();
        }
    }

    pub fn selected_node(&self) -> Option<&NodeSummary> {
        self.nodes.get(self.selected)
    }

    pub fn select_next(&mut self) {
        if !self.nodes.is_empty() {
            self.selected = (self.selected + 1) % self.nodes.len();
        }
    }

    pub fn select_previous(&mut self) {
        if !self.nodes.is_empty() {
            self.selected = self.selected.checked_sub(1).unwrap_or(self.nodes.len() - 1);
        }
    }

    /// Keeps the cursor on a row that exists after the mesh changed size.
    fn clamp_selection(&mut self) {
        if self.nodes.is_empty() {
            self.selected = 0;
        } else if self.selected >= self.nodes.len() {
            self.selected = self.nodes.len() - 1;
        }
    }

    pub fn poll_faster(&mut self) {
        self.poll = (self.poll / 2).max(MIN_POLL);
    }

    pub fn poll_slower(&mut self) {
        self.poll = (self.poll * 2).min(MAX_POLL);
    }

    /// Pre-fills the form's constraints from the selected node's labels, so
    /// "send this to that machine" is one keystroke rather than typing.
    pub fn open_form(&mut self) {
        if self.form.constraints.is_empty()
            && let Some(node) = self.selected_node()
            && let Some((key, value)) = node.labels.iter().next()
        {
            self.form.constraints = format!("{key}={value}");
        }
        self.mode = Mode::Submitting;
    }

    /// Applies one key press to the form. Returns the task to submit, if any.
    pub fn edit_form(&mut self, key: Key) -> Option<Submission> {
        match key {
            Key::Escape => {
                self.mode = Mode::Watching;
                None
            }
            Key::Tab => {
                self.form.focus = self.form.focus.next();
                None
            }
            Key::Left | Key::Right => {
                if self.form.focus == Field::Priority {
                    self.form.shift_priority(matches!(key, Key::Right));
                }
                None
            }
            Key::Backspace => {
                if self.form.focus != Field::Priority {
                    self.form.field_mut().pop();
                }
                None
            }
            Key::Char(character) => {
                if self.form.focus != Field::Priority {
                    self.form.field_mut().push(character);
                }
                None
            }
            Key::Enter => self.finish_form(),
        }
    }

    fn finish_form(&mut self) -> Option<Submission> {
        let payload = match self.form.payload_bytes() {
            Ok(payload) => payload,
            Err(message) => {
                self.push_log(message, LineKind::Bad);
                return None;
            }
        };

        let kind = self.form.kind.trim().to_string();
        if kind.is_empty() {
            self.push_log("a task needs a kind", LineKind::Bad);
            return None;
        }

        let constraints = self.form.constraint_list();
        self.mode = Mode::Watching;
        self.submitting = true;
        self.push_log(
            match constraints.is_empty() {
                true => format!("submitting {kind} ({})", self.form.priority),
                false => format!(
                    "submitting {kind} ({}) where {}",
                    self.form.priority,
                    constraints.join(", ")
                ),
            },
            LineKind::Info,
        );

        Some(Submission {
            kind,
            payload,
            constraints,
            priority: self.form.priority,
        })
    }
}

/// A task the operator asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub kind: String,
    pub payload: Vec<u8>,
    pub constraints: Vec<String>,
    pub priority: Priority,
}

/// The keys the form understands, named so the state machine does not depend
/// on the terminal backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Key {
    Char(char),
    Backspace,
    Enter,
    Tab,
    Escape,
    Left,
    Right,
}

/// First segment of a UUID — enough to tell nodes apart on one screen.
pub fn short(id: &str) -> &str {
    id.split('-').next().unwrap_or(id)
}

/// Bytes as something a person reads at a glance.
pub fn bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = value as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{value} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(hostname: &str) -> NodeSummary {
        NodeSummary {
            node_id: format!("{hostname}-1111-2222"),
            hostname: hostname.to_string(),
            cpu_cores: 4,
            cpu_usage: 0.1,
            memory_usage: 0.2,
            labels: Default::default(),
            address: "127.0.0.1:7001".to_string(),
            latency_ms: None,
            bandwidth_bytes_per_sec: None,
            datasets_held: 0,
            bytes_held: 0,
            connected: true,
        }
    }

    fn app() -> App {
        App::new("127.0.0.1:7100".to_string(), Duration::from_secs(1))
    }

    #[test]
    fn the_first_sample_has_no_rate_to_report() {
        let mut throughput = Throughput::default();
        let now = Instant::now();
        throughput.record(1_000_000, now);

        // A cumulative counter's first reading is not a measurement of a rate.
        assert_eq!(throughput.latest(), 0);
    }

    #[test]
    fn a_rate_is_the_difference_over_the_gap() {
        let mut throughput = Throughput::default();
        let start = Instant::now();
        throughput.record(1_000, start);
        throughput.record(3_000, start + Duration::from_secs(2));

        assert_eq!(throughput.latest(), 1_000);
        assert_eq!(throughput.samples(), vec![0, 1_000]);
    }

    #[test]
    fn a_restarted_controller_reads_as_zero_rather_than_a_spike() {
        let mut throughput = Throughput::default();
        let start = Instant::now();
        throughput.record(9_000, start);
        // The controller restarted and is counting from the beginning again.
        throughput.record(10, start + Duration::from_secs(1));

        assert_eq!(throughput.latest(), 0, "a negative rate is not a huge one");
    }

    #[test]
    fn the_graph_keeps_a_bounded_window() {
        let mut throughput = Throughput::default();
        let start = Instant::now();
        for index in 0..(HISTORY as u64 + 50) {
            throughput.record(index * 1_000, start + Duration::from_secs(index));
        }

        assert_eq!(throughput.samples().len(), HISTORY);
        assert_eq!(throughput.peak(), 1_000);
    }

    #[test]
    fn a_node_leaving_does_not_leave_the_cursor_past_the_end() {
        let mut app = app();
        app.apply_nodes(vec![summary("a"), summary("b"), summary("c")]);
        app.selected = 2;

        app.apply_nodes(vec![summary("a")]);

        assert_eq!(app.selected, 0);
        assert!(app.selected_node().is_some());
    }

    #[test]
    fn nodes_are_shown_in_a_stable_order() {
        let mut app = app();
        app.apply_nodes(vec![summary("rpi4"), summary("desktop"), summary("cloud")]);

        let order: Vec<_> = app.nodes.iter().map(|node| node.hostname.clone()).collect();
        assert_eq!(order, ["cloud", "desktop", "rpi4"]);
    }

    #[test]
    fn selection_wraps_in_both_directions() {
        let mut app = app();
        app.apply_nodes(vec![summary("a"), summary("b")]);

        app.select_previous();
        assert_eq!(app.selected, 1, "wrapping backwards from the first row");
        app.select_next();
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn selecting_an_empty_mesh_does_not_panic() {
        let mut app = app();
        app.select_next();
        app.select_previous();
        assert_eq!(app.selected, 0);
        assert!(app.selected_node().is_none());
    }

    #[test]
    fn a_lost_controller_keeps_the_last_numbers_on_screen() {
        let mut app = app();
        app.apply_nodes(vec![summary("worker")]);

        app.lose("connection reset");

        // Blanking the table would make "the mesh went quiet" look exactly
        // like "every node left", which are different emergencies.
        assert_eq!(app.nodes.len(), 1);
        assert!(matches!(app.connection, LinkState::Lost(_)));
    }

    #[test]
    fn the_same_failure_is_not_logged_over_and_over() {
        let mut app = app();
        for _ in 0..5 {
            app.lose("connection refused");
        }
        assert_eq!(app.log.len(), 1, "one poll failure per second is not news");
    }

    #[test]
    fn a_cpu_task_takes_a_count_and_sends_eight_bytes() {
        let mut app = app();
        app.form.kind = "cpu".to_string();
        app.form.payload = "5000000".to_string();

        let submission = app.edit_form(Key::Enter).expect("a valid form");
        assert_eq!(submission.payload, 5_000_000u64.to_le_bytes().to_vec());
    }

    #[test]
    fn a_cpu_task_that_is_not_a_number_is_refused_before_it_is_sent() {
        let mut app = app();
        app.form.kind = "cpu".to_string();
        app.form.payload = "lots".to_string();

        assert!(app.edit_form(Key::Enter).is_none());
        assert_eq!(app.log.front().map(|line| line.kind), Some(LineKind::Bad));
        assert!(!app.submitting, "nothing was sent");
    }

    #[test]
    fn constraints_are_split_on_commas_or_spaces() {
        let mut app = app();
        app.form.constraints = "gpu=true, region!=us-east  nvme".to_string();

        let submission = app.edit_form(Key::Enter).expect("a valid form");
        assert_eq!(
            submission.constraints,
            ["gpu=true", "region!=us-east", "nvme"]
        );
    }

    #[test]
    fn typing_goes_to_the_focused_field_and_tab_moves_on() {
        let mut app = app();
        app.form.kind.clear();

        app.edit_form(Key::Char('h'));
        app.edit_form(Key::Char('a'));
        app.edit_form(Key::Char('x'));
        app.edit_form(Key::Backspace);
        app.edit_form(Key::Tab);
        app.form.payload.clear();
        app.edit_form(Key::Char('z'));

        assert_eq!(app.form.kind, "ha");
        assert_eq!(app.form.payload, "z");
    }

    #[test]
    fn escape_abandons_the_form_without_submitting() {
        let mut app = app();
        app.mode = Mode::Submitting;

        assert!(app.edit_form(Key::Escape).is_none());
        assert_eq!(app.mode, Mode::Watching);
        assert!(!app.submitting);
    }

    #[test]
    fn opening_the_form_offers_the_selected_node_as_a_constraint() {
        let mut app = app();
        let mut node = summary("gpu-box");
        node.labels.insert("kind".to_string(), "gpu".to_string());
        app.apply_nodes(vec![node]);

        app.open_form();

        // "run it there" should be one keystroke, not typing a label from memory.
        assert_eq!(app.form.constraints, "kind=gpu");
    }

    #[test]
    fn the_poll_interval_stays_inside_its_bounds() {
        let mut app = app();
        for _ in 0..12 {
            app.poll_faster();
        }
        assert_eq!(app.poll, MIN_POLL);

        for _ in 0..12 {
            app.poll_slower();
        }
        assert_eq!(app.poll, MAX_POLL);
    }

    #[test]
    fn a_result_says_where_the_task_actually_ran() {
        let mut app = app();
        app.submitting = true;
        app.apply_result(Finished {
            node_id: "4f3cb68b-0000-1111".to_string(),
            success: true,
            output: Vec::new(),
            duration_ms: 6.7,
            error: None,
        });

        let line = app.log.front().expect("a line");
        assert_eq!(line.kind, LineKind::Good);
        assert!(line.text.contains("4f3cb68b"), "{}", line.text);
        assert!(!app.submitting);
    }

    #[test]
    fn a_task_that_ran_and_failed_is_reported_rather_than_looking_like_success() {
        let mut app = app();
        app.apply_result(Finished {
            node_id: "4f3cb68b-0000-1111".to_string(),
            success: false,
            output: Vec::new(),
            duration_ms: 1.0,
            error: Some("unknown task kind".to_string()),
        });

        let line = app.log.front().expect("a line");
        assert_eq!(line.kind, LineKind::Bad);
        assert!(line.text.contains("unknown task kind"), "{}", line.text);
    }

    #[test]
    fn byte_counts_are_readable() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(999), "999 B");
        assert_eq!(bytes(1024), "1.0 KiB");
        assert_eq!(bytes(4 * 1024 * 1024), "4.0 MiB");
        assert_eq!(bytes(1536), "1.5 KiB");
    }

    #[test]
    fn a_node_id_is_shortened_to_something_a_screen_fits() {
        assert_eq!(short("4f3cb68b-1234-5678-9abc-def012345678"), "4f3cb68b");
        assert_eq!(short("plain"), "plain");
    }
}
