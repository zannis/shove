//! Tests for the benchmark chart generator.
//!
//! The generator lives at `examples/common/chartgen.rs` and is pulled into
//! `examples/chartgen.rs` with `#[path]`. Cargo defaults `[[example]]` targets
//! to `test = false`, so a `#[cfg(test)]` module inside the example would never
//! be compiled into a test binary. Including it here — in a real
//! integration-test target — is what makes these run.
//!
//! Deliberately **not** feature-gated. `chartgen` carries no
//! `required-features` because chart regeneration needs no backend and runs
//! under `--no-default-features`; gating this file would mean the enforcement
//! rules go untested in exactly the configuration CI uses.

#[path = "../examples/common/chartgen.rs"]
mod chartgen;

use chartgen::{ChartError, Document, Family, Mode};
use plotters::style::RGBColor;

/// The x band that can only hold y-axis tick labels: the frame's 16px left
/// margin plus the 74px `y_label_area_size` — the real plot-left in the
/// line charts. Chartgen does not export those, so this is a hand-synced
/// copy; it sits exactly at plot-left (no slack) so no plot-interior text
/// (a category label on a dense sweep) can leak into the probes filtering
/// on it.
const Y_TICK_BAND_X: f64 = 90.0;

/// A minimal document that satisfies every rule, used as the base for the
/// mutations below. Written as JSON rather than built through the structs so
/// the tests exercise the real deserialisation path.
fn document(runs: &str) -> String {
    format!(
        r#"{{
          "schema_version": 4,
          "generated_at": "2026-08-31T22:10:30Z",
          "shove_version": "0.14.0",
          "rust_version": "rustc 1.91.1",
          "hardware": {{
            "label": "aarch64 (6c / 15 GB)",
            "cpu": "aarch64",
            "physical_cores": 6,
            "ram_gb": 15,
            "os": "Debian GNU/Linux 13"
          }},
          "runs": [{runs}]
        }}"#
    )
}

/// A v4 row with an explicit `handler_cost` marker.
///
/// The extra keys (tier, messages, percentiles, `setup_secs`, …) exist only
/// to mirror the harness's real output shape; chartgen does not read them.
fn scenario_with_cost(
    flow: &str,
    mode: &str,
    payload: u64,
    consumers: u32,
    throughput: f64,
    handler_cost: &str,
) -> String {
    scenario_with_window(
        flow,
        mode,
        payload,
        consumers,
        throughput,
        handler_cost,
        2.0,
    )
}

/// A v4 row with an explicit marker *and* an explicit measured window.
#[allow(clippy::too_many_arguments)]
fn scenario_with_window(
    flow: &str,
    mode: &str,
    payload: u64,
    consumers: u32,
    throughput: f64,
    handler_cost: &str,
    duration_secs: f64,
) -> String {
    // Since v3 every `consume_batch` row carries its batch knobs, and no
    // other flow's row may carry them.
    let batch_knobs = if flow == "consume_batch" {
        r#""max_batch_size": 500, "max_batch_age_ms": 200,"#
    } else {
        ""
    };
    // The marker is derived from the handler profile, so a sleeping-handler
    // marker needs a sleeping handler's label.
    let handler = if handler_cost == "handler_bound" || handler_cost == "handler_amortised" {
        "slow (50-300ms)"
    } else {
        "zero (no-op)"
    };
    // Barrier-less flows never record a setup window; the harness emits
    // `null` there.
    let setup_secs = if flow == "consume_fifo" || flow == "dlq_drain" {
        "null"
    } else {
        "0.4"
    };
    format!(
        r#"{{
          "flow": "{flow}", "mode": "{mode}", "payload_bytes": {payload},
          "tier": "moderate", "messages": 5000, "consumers": {consumers},
          "handler": "{handler}",
          "handler_cost": "{handler_cost}",
          "setup_secs": {setup_secs},
          {batch_knobs}
          "throughput_msg_per_sec": {throughput},
          "dispatch_p50_ms": 1.5, "dispatch_p95_ms": 4.0, "dispatch_p99_ms": 9.0,
          "e2e_p50_ms": 1.5, "e2e_p95_ms": 4.0, "e2e_p99_ms": 9.0,
          "scaling_efficiency": 1.0, "peak_rss_mb": 1.0, "cpu_pct": 100.0,
          "duration_secs": {duration_secs:?}
        }}"#
    )
}

/// A v4 row whose marker is what the harness would derive for a zero handler:
/// publish flows carry `no_handler`, the barrier-less flows (`consume_fifo`,
/// `dlq_drain`) carry `setup_bound`, everything else is a
/// `framework` row with a separated window.
fn scenario(flow: &str, mode: &str, payload: u64, consumers: u32, throughput: f64) -> String {
    let cost = match flow {
        "publish_single" | "publish_batch" => "no_handler",
        "consume_fifo" | "dlq_drain" => "setup_bound",
        _ => "framework",
    };
    scenario_with_cost(flow, mode, payload, consumers, throughput, cost)
}

/// An in-process run covering the slices every chart family reads.
fn inmemory_run(representative: bool) -> String {
    let mut rows = Vec::new();
    for consumers in [1u32, 2, 4] {
        rows.push(scenario(
            "consume_parallel",
            "parallel",
            1024,
            consumers,
            10_000.0 * f64::from(consumers),
        ));
    }
    for payload in [64u64, 1024, 65536] {
        rows.push(scenario(
            "consume_parallel",
            "parallel",
            payload,
            1,
            10_000.0,
        ));
        rows.push(scenario("consume_fifo", "fifo", payload, 1, 4_000.0));
        rows.push(scenario("publish_single", "parallel", payload, 1, 50_000.0));
    }
    format!(
        r#"{{
          "backend": "inmemory",
          "broker": {{ "name": "in-process", "version": "n/a", "deployment": "in-process" }},
          "representative": {representative},
          "results": [{}],
          "failures": [],
          "unsupported": [
            {{ "flow": "consume_batch", "reason": "run_batch is Kafka-only" }}
          ]
        }}"#,
        rows.join(",")
    )
}

fn parse(raw: &str) -> Document {
    serde_json::from_str(raw).expect("fixture should deserialise")
}

fn render_all(doc: &Document) -> Vec<(Family, String)> {
    Family::ALL
        .into_iter()
        .map(|f| {
            let svg = chartgen::render_to_string(doc, f, Mode::Light)
                .unwrap_or_else(|e| panic!("{f:?} should render: {e}"));
            (f, svg)
        })
        .collect()
}

// ── Rule 1: an unknown schema_version is a hard error ───────────────────────

#[test]
fn unknown_schema_version_is_rejected() {
    // An *older* version is refused the same as a newer one: through v3 a
    // row's duration included the group-join latency, so its throughput is
    // not a drain rate and must never reach a v4 axis.
    let raw =
        document(&inmemory_run(true)).replace("\"schema_version\": 4", "\"schema_version\": 2");
    let doc = parse(&raw);

    match chartgen::validate(&doc) {
        Err(ChartError::UnsupportedSchemaVersion { found, expected }) => {
            assert_eq!(found, 2);
            assert_eq!(expected, 4);
        }
        other => panic!("expected UnsupportedSchemaVersion, got {other:?}"),
    }
}

#[test]
fn unknown_schema_version_blocks_every_chart() {
    // The rule has to bite at the render entry point too, not only in a
    // validate() a caller might forget: a document from another schema must
    // not be able to produce a single chart.
    let raw =
        document(&inmemory_run(true)).replace("\"schema_version\": 4", "\"schema_version\": 7");
    let doc = parse(&raw);

    for family in Family::ALL {
        assert!(
            matches!(
                chartgen::render_to_string(&doc, family, Mode::Light),
                Err(ChartError::UnsupportedSchemaVersion { .. })
            ),
            "{family:?} rendered a chart from an unknown schema version"
        );
    }
}

// ── Rule 6: an unclassifiable handler_cost is a hard error ──────────────────

#[test]
fn an_unknown_handler_cost_is_rejected_and_blocks_every_chart() {
    // Every publishability decision keys on the marker, so a value outside
    // the closed set would silently fall out of every filter — a row dropped
    // by filter reads exactly like a row that never existed.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario_with_cost(
            "consume_parallel",
            "parallel",
            1024,
            1,
            10_000.0,
            "warp_speed"
        )
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));

    match chartgen::validate(&doc) {
        Err(ChartError::UnknownHandlerCost {
            backend,
            flow,
            value,
        }) => {
            assert_eq!(backend, "kafka");
            assert_eq!(flow, "consume_parallel");
            assert_eq!(value, "warp_speed");
        }
        other => panic!("expected UnknownHandlerCost, got {other:?}"),
    }
    for family in Family::ALL {
        assert!(
            matches!(
                chartgen::render_to_string(&doc, family, Mode::Light),
                Err(ChartError::UnknownHandlerCost { .. })
            ),
            "{family:?} rendered a chart from an unclassifiable row"
        );
    }
}

#[test]
fn a_missing_handler_cost_on_a_v4_row_is_rejected() {
    // A v4 document whose rows lack the marker deserialises (the field
    // defaults so *older* documents fail by version, which is the legible
    // refusal) — but on a current-version row the absent marker must then be
    // refused here, not defaulted into "not framework" and quietly filtered.
    let raw = document(&inmemory_run(true)).replace(r#""handler_cost": "framework","#, "");
    assert!(
        !raw.contains("\"handler_cost\": \"framework\""),
        "the fixture edit should have removed the framework markers"
    );
    let doc = parse(&raw);

    match chartgen::validate(&doc) {
        Err(ChartError::UnknownHandlerCost { value, .. }) => assert_eq!(value, ""),
        other => panic!("expected UnknownHandlerCost for a missing marker, got {other:?}"),
    }
}

// ── Marker semantics: what each handler_cost may and may not produce ────────

#[test]
fn a_setup_bound_row_never_plots_an_absolute_drain_rate() {
    // A representative backend whose consume_parallel windows could not be
    // separated: the throughput is a mixture of setup and drain. The line
    // charts must not plot it — and must say why the backend is absent,
    // because a silent absence reads as "we forgot to measure it".
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{},{}], "failures": [], "unsupported": []
        }}"#,
        // No recorded setup window: the driver never separated setup from
        // drain, which is the long-window way a barrier flow ends up
        // setup-bound.
        scenario_with_cost(
            "consume_parallel",
            "parallel",
            1024,
            1,
            424_242.0,
            "setup_bound"
        )
        .replace("\"setup_secs\": 0.4", "\"setup_secs\": null"),
        scenario_with_cost(
            "consume_parallel",
            "parallel",
            64,
            1,
            434_343.0,
            "setup_bound"
        )
        .replace("\"setup_secs\": 0.4", "\"setup_secs\": null")
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));

    for family in [Family::ThroughputVsConsumers, Family::ThroughputVsPayload] {
        let svg =
            chartgen::render_to_string(&doc, family, Mode::Light).expect("chart should render");
        for magnitude in ["424242", "424.2k", "434343", "434.3k"] {
            assert!(
                !svg.contains(magnitude),
                "{family:?} published the setup-bound magnitude {magnitude}"
            );
        }
        let caption = texts(&svg)
            .into_iter()
            .map(|(_, _, _, t)| t)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            caption.contains("kafka: measured, but no drain rate")
                && caption.contains("coordination cost"),
            "{family:?} must name the backend whose slice is setup-bound only: {caption}"
        );
        assert!(
            svg.contains("no drain rate is published"),
            "{family:?} must say why no value is plotted"
        );
    }
}

#[test]
fn the_sequenced_bar_is_a_labelled_lower_bound() {
    // Sequenced consume holds no readiness barrier, so its rows are always
    // setup-bound. The ordering-cost chart keeps the bar — losing it would
    // gut the one chart that shows what ordering costs — but as a muted,
    // caption-qualified lower bound rather than a false absolute.
    let svg = chartgen::render_to_string(
        &parse(&document(&inmemory_run(true))),
        Family::ParallelVsSequenced,
        Mode::Light,
    )
    .expect("chart should render");

    assert!(
        svg.contains("lower bound"),
        "the sequenced bar must be labelled a lower bound"
    );
    assert!(
        svg.contains("at least the bar shown"),
        "the caption must say which direction the bound points"
    );
}

#[test]
fn a_published_batch_bar_names_its_knobs() {
    // Since v3 the batch knobs are what distinguish two otherwise
    // byte-identical `consume_batch` rows, so a chart that publishes a batch
    // number without them is a bar with no stated batch size.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_batch", "batch", 64, 1, 80_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));

    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    assert!(
        svg.contains("up to 500 messages or 200 ms per batch"),
        "the batch bar must state the knobs it ran with"
    );
}

#[test]
fn a_sleeping_handler_row_never_reaches_a_throughput_chart() {
    // A handler_bound row's throughput is the simulated sleep, not shove.
    // Its magnitude must not appear in any throughput family — not as a bar,
    // not as a line point, and not by stretching the axis.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{},{}], "failures": [], "unsupported": []
        }}"#,
        scenario_with_cost(
            "consume_parallel",
            "parallel",
            1024,
            1,
            30_000.0,
            "framework"
        ),
        scenario_with_cost(
            "consume_parallel",
            "parallel",
            1024,
            1,
            9_876_543.0,
            "handler_bound"
        )
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));

    for family in [
        Family::ThroughputVsConsumers,
        Family::ThroughputVsPayload,
        Family::ParallelVsSequenced,
    ] {
        let svg =
            chartgen::render_to_string(&doc, family, Mode::Light).expect("chart should render");
        for magnitude in ["9876543", "9.9M", "9.8M", "11.3M", "11M"] {
            assert!(
                !svg.contains(magnitude),
                "{family:?} let a sleeping-handler magnitude reach the chart: {magnitude}"
            );
        }
    }
}

// ── Rule 2: representative:false never plots an absolute value ──────────────

#[test]
fn non_representative_backend_never_plots_an_absolute_value() {
    // A LocalStack-shaped run whose throughputs are deliberately far larger
    // than the representative backend's, so if any of them reached the axis
    // the axis maximum would move.
    let sqs = format!(
        r#"{{
          "backend": "sqs",
          "broker": {{ "name": "LocalStack", "version": "3", "deployment": "docker" }},
          "representative": false,
          "results": [{}],
          "failures": [],
          "unsupported": []
        }}"#,
        [
            scenario("consume_parallel", "parallel", 1024, 1, 987_654.0),
            scenario("consume_parallel", "parallel", 1024, 2, 876_543.0),
            scenario("consume_parallel", "parallel", 64, 1, 765_432.0),
            scenario("consume_fifo", "fifo", 1024, 1, 654_321.0),
        ]
        .join(",")
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), sqs)));

    for (family, svg) in render_all(&doc) {
        // No magnitude of the non-representative run may appear anywhere in
        // the file — not as a datum, not as an axis tick, not as a label.
        // This holds for every family, including the in-process-only one.
        for magnitude in ["987654", "876543", "765432", "654321", "987.7k", "988k"] {
            assert!(
                !svg.contains(magnitude),
                "{family:?} leaked the non-representative magnitude {magnitude}"
            );
        }

        if family == Family::FrameworkOverhead {
            // This family is the in-process backend by definition — "what does
            // shove itself cost" is only meaningful with the broker removed.
            // It does not silently drop the other backends so much as never
            // claim to cover them, so it must say so on its face.
            assert!(
                svg.contains("in-process") && svg.contains("broker removed"),
                "the framework-overhead chart must state its in-process-only scope"
            );
            continue;
        }
        if family == Family::DispatchLatency {
            // The refusal panel publishes no run at all, so there is no bar
            // to label; the magnitude-leak assertions above still hold.
            assert!(
                svg.contains("no publishable dispatch-latency measurement"),
                "the latency panel must state its refusal"
            );
            continue;
        }

        assert!(
            svg.contains("shape only") && svg.contains("not representative"),
            "{family:?} must label the non-representative run"
        );
        assert!(
            svg.contains("LocalStack, not AWS"),
            "{family:?} must say why the run is not representative"
        );
    }
}

#[test]
fn a_non_representative_run_does_not_stretch_the_axis() {
    let base = parse(&document(&inmemory_run(true)));
    let with_sqs = parse(&document(&format!(
        "{},{}",
        inmemory_run(true),
        format_args!(
            r#"{{
              "backend": "sqs", "representative": false,
              "results": [{}], "failures": [], "unsupported": []
            }}"#,
            scenario("consume_parallel", "parallel", 1024, 1, 5_000_000.0)
        )
    )));

    // Adding a 5M msg/s non-representative series must not change the axis of
    // the representative data. If it did, every published number would be
    // squashed into the bottom of a chart scaled by LocalStack.
    // The y tick labels ARE the axis: extract every tick in the y-label
    // area and compare the sets. Counting two hard-coded linear tick strings
    // would go vacuously green on the log axis (zero == zero proves
    // nothing); the full set comparison survives any axis shape. The x-band
    // alone is not enough — the title and caption block are left-anchored in
    // the same band, and the sqs document legitimately grows a caption line —
    // so the probe also requires the fmt_count shape (digits, '.', k/M).
    let axis_of = |doc: &Document| -> Vec<String> {
        let svg = chartgen::render_to_string(doc, Family::ThroughputVsConsumers, Mode::Light)
            .expect("chart should render");
        let ticks: Vec<String> = texts(&svg)
            .into_iter()
            .filter(|(x, _, _, c)| *x < Y_TICK_BAND_X && tick_shaped(c))
            .map(|(_, _, _, c)| c)
            .collect();
        assert!(
            !ticks.is_empty(),
            "no y tick labels found — the probe went blind"
        );
        ticks
    };
    assert_eq!(
        axis_of(&base),
        axis_of(&with_sqs),
        "a non-representative series changed the representative axis"
    );
}

// ── Rule 3: unsupported[] is an explicit marker, never a zero ───────────────

#[test]
fn unsupported_flow_is_marked_not_zeroed_and_not_dropped() {
    let svg = chartgen::render_to_string(
        &parse(&document(&inmemory_run(true))),
        Family::FrameworkOverhead,
        Mode::Light,
    )
    .expect("chart should render");

    // The flow is on the axis (not silently dropped, which reads as "we
    // forgot"), carries the not-supported marker, and its reason is stated.
    assert!(
        svg.contains("consume_batch"),
        "unsupported flow left the axis"
    );
    assert!(svg.contains("n/s"), "no not-supported marker drawn");
    assert!(
        svg.contains("run_batch is Kafka-only"),
        "the reason from unsupported[] is not rendered"
    );
    // The caption is the document's own reason, verbatim: a declared entry
    // whose reason says "not measured" must not be promoted into a
    // "not supported" library claim by caption prose.
    assert!(
        !svg.contains("not supported —"),
        "chartgen must not prefix its own capability claim onto the reason"
    );
}

#[test]
fn a_supported_but_unmeasured_flow_is_named_not_dropped() {
    // `supervisor` is supported on every backend and is not in the fixture's
    // `unsupported[]`. Drawing an "n/s" column would claim a capability hole
    // that does not exist; dropping it without a word is the omission the
    // marker rule exists to prevent. It must be named in the caption.
    let svg = chartgen::render_to_string(
        &parse(&document(&inmemory_run(true))),
        Family::FrameworkOverhead,
        Mode::Light,
    )
    .expect("chart should render");

    // Slice-scoped on purpose: "not measured in this run" was a false claim
    // for a flow measured at coordinates outside the chart's slice.
    assert!(
        svg.contains("no measurement for this slice in this run"),
        "supported-but-unmeasured flows are dropped without a word"
    );
    assert!(
        svg.contains("supervisor"),
        "the unmeasured flow is not named"
    );
}

#[test]
fn a_backend_absent_from_a_slice_is_named_rather_than_omitted() {
    // A backend measured for one flow but not the chart's slice must still be
    // accounted for in words. Omission is the failure mode rule 3 exists for.
    let sparse = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [],
          "unsupported": [{{ "flow": "broadcast", "reason": "not measured on this host" }}]
        }}"#,
        scenario("publish_single", "parallel", 64, 1, 1_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), sparse)));

    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("should render");
    // The wording matters as much as the mention: this is a gap in the run,
    // and captioning it "not supported" would publish a false capability
    // claim about the library on the chart's face.
    assert!(
        svg.contains("kafka: no measurement for this slice"),
        "a backend outside the slice was dropped without a word"
    );
    assert!(
        svg.contains("not a capability hole"),
        "the caption must say the absence is a gap, not a missing capability"
    );
    assert!(
        !svg.contains("kafka: not supported"),
        "a mere gap must never be published as a capability hole"
    );
}

// ── Rule 4: provenance in every caption ─────────────────────────────────────

#[test]
fn every_chart_renders_the_provenance_block() {
    let doc = parse(&document(&inmemory_run(true)));
    for (family, svg) in render_all(&doc) {
        for token in [
            "2026-08-31T22:10:30Z",
            "0.14.0",
            "aarch64 (6c / 15 GB)",
            "rustc 1.91.1",
            // The handler profile the dataset ran under is provenance too: a
            // no-op-handler dataset is a deliberate choice, stated on the
            // artifact rather than in a PR thread.
            "handler: zero (no-op)",
        ] {
            assert!(
                svg.contains(token),
                "{family:?} caption is missing provenance token {token}"
            );
        }
    }
}

// ── Rule 5: an empty results[] that is not fully declared is a hard error ───

#[test]
fn silently_empty_run_is_rejected() {
    let broken = r#"{
      "backend": "redis", "representative": true,
      "results": [], "failures": [],
      "unsupported": [{ "flow": "broadcast", "reason": "declared, but only one of ten" }]
    }"#;
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), broken)));

    match chartgen::validate(&doc) {
        Err(ChartError::SilentlyEmptyRun { backend, missing }) => {
            assert_eq!(backend, "redis");
            assert!(
                missing.contains(&"consume_parallel".to_string()),
                "the undeclared flows should be named: {missing:?}"
            );
            assert!(
                !missing.contains(&"broadcast".to_string()),
                "a declared flow must not be reported as undeclared"
            );
        }
        other => panic!("expected SilentlyEmptyRun, got {other:?}"),
    }
}

#[test]
fn empty_run_is_allowed_when_every_flow_is_declared_unsupported() {
    // The legitimate shape of an empty run: nothing measured *because* nothing
    // is supported. This must not be swept up by rule 5.
    let declared: Vec<String> = chartgen::KNOWN_FLOWS
        .iter()
        .map(|f| format!(r#"{{ "flow": "{f}", "reason": "capability absent on this backend" }}"#))
        .collect();
    let run = format!(
        r#"{{
          "backend": "sqs", "representative": true,
          "results": [], "failures": [], "unsupported": [{}]
        }}"#,
        declared.join(",")
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));

    assert!(
        chartgen::validate(&doc).is_ok(),
        "a fully-declared empty run must be accepted"
    );
}

#[test]
fn framework_overhead_requires_the_in_process_run() {
    // "What does shove itself cost" has no meaning without the broker-free
    // baseline, so a document without one must fail rather than render a
    // chart of whatever backend happens to be present.
    let kafka_only = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 1024, 1, 1_000.0)
    );
    let doc = parse(&document(&kafka_only));

    assert!(
        matches!(
            chartgen::render_to_string(&doc, Family::FrameworkOverhead, Mode::Light),
            Err(ChartError::MissingInProcessRun)
        ),
        "framework overhead rendered without an in-process run"
    );
}

// ── Determinism: the committed-artifact byte-compare depends on it ──────────

#[test]
fn rendering_twice_produces_identical_bytes() {
    let doc = parse(&document(&inmemory_run(true)));
    for family in Family::ALL {
        for mode in Mode::ALL {
            let first = chartgen::render_to_string(&doc, family, mode).expect("first render");
            let second = chartgen::render_to_string(&doc, family, mode).expect("second render");
            assert_eq!(
                first, second,
                "{family:?} ({mode:?}) is not byte-deterministic"
            );
        }
    }
}

#[test]
fn output_carries_no_wall_clock_and_no_external_reference() {
    // A generation timestamp would make the SVG differ on every run by
    // construction, and an external font or stylesheet silently drops under
    // raw.githubusercontent's `default-src 'none'` CSP.
    let doc = parse(&document(&inmemory_run(true)));
    for (family, svg) in render_all(&doc) {
        // Every date-shaped token in the file must be the fixture's own
        // generated_at — a calendar-window check ("no 2026-09") would stop
        // guarding the day the calendar passed it.
        for (i, _) in svg.match_indices("20") {
            let token: String = svg[i..].chars().take(10).collect();
            let bytes = token.as_bytes();
            let is_date = token.len() == 10
                && bytes[..4].iter().all(u8::is_ascii_digit)
                && bytes[4] == b'-'
                && bytes[5..7].iter().all(u8::is_ascii_digit)
                && bytes[7] == b'-'
                && bytes[8..10].iter().all(u8::is_ascii_digit);
            if is_date {
                assert!(
                    doc.generated_at.starts_with(&token),
                    "{family:?} rendered a date that is not the document's: {token}"
                );
            }
        }
        // The SVG namespace declaration is the one legitimate URI in the file:
        // it names the dialect, it is never fetched. Everything else that could
        // pull a byte over the network is banned, because under
        // raw.githubusercontent's `default-src 'none'` CSP it silently drops
        // and the chart renders with pieces missing.
        let body = svg.replace(r#"xmlns="http://www.w3.org/2000/svg""#, "");
        for forbidden in [
            "http://",
            "https://",
            "@import",
            "<script",
            "xlink:href",
            "<image",
            "url(",
        ] {
            assert!(
                !body.contains(forbidden),
                "{family:?} is not self-contained: found {forbidden}"
            );
        }
        assert!(
            svg.contains("sans-serif"),
            "{family:?} should name a generic font family the viewer resolves"
        );
    }
}

#[test]
fn each_variant_paints_its_surface_and_only_its_palette() {
    // Theming is structural: each variant paints exactly one canvas-sized
    // rect — its own surface — and every colour it emits comes from that
    // mode's validated palette (chrome roles, series slots, and the
    // pre-blended muted fills). A colour outside the whitelist is either an
    // eyeballed hex or a leak from the other mode; both are refused.
    let doc = parse(&document(&inmemory_run(true)));
    for mode in Mode::ALL {
        let allowed = mode.allowed_hexes();
        for family in Family::ALL {
            let svg = chartgen::render_to_string(&doc, family, mode)
                .unwrap_or_else(|e| panic!("{family:?} ({mode:?}) should render: {e}"));
            // `rgb(...)` spellings are refused outright so a colour cannot
            // dodge the whitelist by notation.
            assert!(
                !svg.contains("rgb("),
                "{family:?} ({mode:?}) uses an rgb() colour"
            );
            for hex in svg.split('#').skip(1).filter_map(|rest| rest.get(..6)) {
                if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    continue;
                }
                assert!(
                    allowed.contains(&hex.to_uppercase()),
                    "{family:?} ({mode:?}) emits #{hex}, which is not in the \
                     mode's validated palette"
                );
            }
            // Exactly two canvas-sized rects: the painted surface and the
            // hairline border ring. Anything else painting the canvas is a
            // second background.
            let (full_size, _) = full_size_rects(&svg);
            assert_eq!(
                full_size, 2,
                "{family:?} ({mode:?}) should paint exactly its surface and \
                 its border ring"
            );
        }
    }
}

#[test]
fn the_two_variants_differ_in_ink_and_agree_on_content() {
    // The dark file is a selected palette, not a recolor of the light one —
    // but the two variants must say exactly the same thing: same text, same
    // captions, same markers. Only the ink may differ.
    let doc = parse(&document(&inmemory_run(true)));
    for family in Family::ALL {
        let light =
            chartgen::render_to_string(&doc, family, Mode::Light).expect("light should render");
        let dark =
            chartgen::render_to_string(&doc, family, Mode::Dark).expect("dark should render");
        assert_ne!(light, dark, "{family:?}: the dark variant is not themed");
        let words =
            |svg: &str| -> Vec<String> { texts(svg).into_iter().map(|(_, _, _, c)| c).collect() };
        assert_eq!(
            words(&light),
            words(&dark),
            "{family:?}: the two variants disagree about published content"
        );
    }
}

// ── Layout: nothing may run off the canvas ──────────────────────────────────

/// One attribute's raw value out of an SVG element chunk. Every element
/// parser below shares this, so a quoting or format quirk in the emitted
/// SVG gets fixed in one place — each caller keeps its own policy for a
/// missing value.
fn svg_attr(chunk: &str, name: &str) -> Option<String> {
    let key = format!("{name}=\"");
    let start = chunk.find(&key)? + key.len();
    let rest = chunk.get(start..)?;
    let stop = rest.find('"')?;
    rest.get(..stop).map(str::to_string)
}

/// The suite's wide-fallback-font extent model: 0.55em per character,
/// over-estimating a proportional face — the conservative direction for
/// every fits-on-canvas assertion built on it.
fn est_width(size: f64, content: &str) -> f64 {
    content.chars().count() as f64 * size * 0.55
}

/// What a y-axis tick label may look like: `fmt_count`'s output alphabet,
/// including the exponent spellings ("1e-7") that deep sub-unit floors
/// produce, and always starting with a digit — which fences the widened
/// alphabet against a dash-or-e-named fixture backend whose flipped end
/// label could stray into the tick band. Shared by every axis probe so the
/// alphabet cannot drift.
fn tick_shaped(c: &str) -> bool {
    c.starts_with(|ch: char| ch.is_ascii_digit())
        && c.chars()
            .all(|ch| ch.is_ascii_digit() || matches!(ch, '.' | 'k' | 'M' | 'e' | '-'))
}

/// Every `<text>` in the file, as (x, y, font-size, content).
fn texts(svg: &str) -> Vec<(f64, f64, f64, String)> {
    let mut out = Vec::new();
    for chunk in svg.split("<text ").skip(1) {
        let Some(end) = chunk.find("</text>") else {
            continue;
        };
        let (head, body) = chunk.split_at(chunk.find('>').unwrap_or(0));
        let attr = |name: &str| svg_attr(head, name);
        // f64 with a loud failure: an unparseable coordinate silently
        // becoming 0 would vacuously pass every overflow assertion built on
        // this helper.
        let parse_coord = |v: Option<String>| -> f64 {
            v.map(|v| {
                v.parse::<f64>()
                    .unwrap_or_else(|_| panic!("unparseable coordinate {v:?}"))
            })
            .unwrap_or(0.0)
        };
        let x = parse_coord(attr("x"));
        let y = parse_coord(attr("y"));
        let size = attr("font-size")
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let content = body[..end.saturating_sub(head.len())]
            .trim_start_matches('>')
            .trim()
            .to_string();
        out.push((x, y, size, content));
    }
    out
}

/// Every `<circle>` in the file, as (cx, cy, r, fill).
fn circles(svg: &str) -> Vec<(f64, f64, f64, String)> {
    let mut out = Vec::new();
    for chunk in svg.split("<circle ").skip(1) {
        let attr = |name: &str| svg_attr(chunk, name);
        let coord = |v: Option<String>| -> f64 {
            v.and_then(|v| v.parse().ok())
                .unwrap_or_else(|| panic!("circle with an unparseable coordinate"))
        };
        out.push((
            coord(attr("cx")),
            coord(attr("cy")),
            coord(attr("r")),
            attr("fill").unwrap_or_default(),
        ));
    }
    out
}

/// The number of canvas-sized rects in the file. Both background probes
/// share this one definition of "canvas-sized" (the painted surface and the
/// border ring), so a regression cannot pass one test and fail the other by
/// a diverged threshold.
fn full_size_rects(svg: &str) -> (usize, usize) {
    let mut full = 0;
    let mut total = 0;
    for rect in svg.split("<rect ").skip(1) {
        // Missing-value policy here: an unparseable dimension counts as 0
        // (not canvas-sized), which errs toward failing the "exactly two
        // canvas-sized rects" assertion loudly.
        let attr = |name: &str| -> f64 {
            svg_attr(rect, name)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0)
        };
        total += 1;
        if attr("width") >= f64::from(chartgen::WIDTH) * 0.9
            && attr("height") >= f64::from(chartgen::HEIGHT) * 0.9
        {
            full += 1;
        }
    }
    (full, total)
}

#[test]
fn no_text_runs_off_the_canvas() {
    // An `unsupported[]` reason is a full sentence. Left unwrapped it walks off
    // the right edge, and a reason the reader cannot see is the same as no
    // reason — which is the failure rule 3 exists to prevent.
    let long_reason = "SQS has no per-process fan-out primitive that shove manages, because \
                       per-process fan-out there needs a real queue plus an SNS subscription \
                       whose lifecycle shove does not own, and a leaked queue costs money forever";
    let run = format!(
        r#"{{
          "backend": "sqs", "representative": true,
          "results": [{}], "failures": [],
          "unsupported": [{{ "flow": "consume_parallel", "reason": "{long_reason}" }}]
        }}"#,
        scenario("publish_single", "parallel", 64, 1, 10.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));

    for (family, svg) in render_all(&doc) {
        for (x, y, size, content) in texts(&svg) {
            assert!(
                (0.0..=f64::from(chartgen::HEIGHT)).contains(&y),
                "{family:?}: text baseline y={y} is outside the canvas: {content:?}"
            );
            // A start-anchored run of text must end inside the canvas.
            let width = est_width(size, &content);
            if x == 24.0 {
                assert!(
                    x + width <= f64::from(chartgen::WIDTH),
                    "{family:?}: text runs to {:.0}px, past the {}px edge: {content:?}",
                    x + width,
                    chartgen::WIDTH
                );
            }
            // Center-anchored text (the refusal panel's block, category
            // labels) escapes the start-anchored check above; hold its
            // half-extent inside both edges.
            if x == f64::from(chartgen::WIDTH) / 2.0 {
                assert!(
                    x - width / 2.0 >= 0.0 && x + width / 2.0 <= f64::from(chartgen::WIDTH),
                    "{family:?}: centered text spans past a canvas edge: {content:?}"
                );
            }
        }
    }
}

#[test]
fn long_reasons_are_wrapped_not_truncated() {
    // Wrapping must preserve the whole reason: dropping the tail would hide
    // exactly the part that explains the capability hole.
    let reason = "run_batch is implemented only for the Kafka backend; no other backend \
                  exposes a batch consume primitive";
    let run = format!(
        r#"{{
          "backend": "nats", "representative": true,
          "results": [{}], "failures": [],
          "unsupported": [{{ "flow": "consume_parallel", "reason": "{reason}" }}]
        }}"#,
        scenario("publish_single", "parallel", 64, 1, 10.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("renders");

    let joined: String = texts(&svg)
        .into_iter()
        .map(|(_, _, _, c)| c)
        .collect::<Vec<_>>()
        .join(" ");
    for fragment in ["run_batch is implemented only", "batch consume primitive"] {
        assert!(
            joined.contains(fragment),
            "the reason lost {fragment:?} — wrapped text must keep every word"
        );
    }
}

// ── The version gate fires before typed deserialisation ─────────────────────

#[test]
fn a_foreign_version_is_refused_by_version_even_when_fields_are_missing() {
    // A v2 document may lack fields v4 requires. The refusal must name the
    // version — the actual problem — not whichever missing field serde
    // happens to trip on first.
    let raw = r#"{ "schema_version": 2, "runs": [ { "backend": "kafka" } ] }"#;
    match chartgen::parse_str(raw) {
        Err(ChartError::UnsupportedSchemaVersion { found, expected }) => {
            assert_eq!(found, 2);
            assert_eq!(expected, 4);
        }
        other => panic!("expected UnsupportedSchemaVersion, got {other:?}"),
    }
}

// ── Per-row invariants are hard errors ───────────────────────────────────────

#[test]
fn a_zero_throughput_row_is_rejected() {
    // A zero rate is a failed measurement wearing a row's clothes — the
    // harness records those in failures[], so one in results[] must refuse,
    // not be quietly reclassified as "not measured".
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 0.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { backend, what, .. }) => {
            assert_eq!(backend, "kafka");
            assert!(what.contains("throughput"), "wrong invariant named: {what}");
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

#[test]
fn an_empty_handler_label_is_rejected() {
    // The provenance line states the handler profile on every chart; a row
    // without the label would silently un-state that disclosure.
    let raw =
        document(&inmemory_run(true)).replace(r#""handler": "zero (no-op)","#, r#""handler": "","#);
    let doc = parse(&raw);
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(
                what.contains("handler label"),
                "wrong invariant named: {what}"
            );
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

#[test]
fn batch_knobs_must_match_the_row_flow_in_both_directions() {
    // A knob-less consume_batch row publishes a bar with no stated batch
    // size; knobs on a non-batch row caption a batch that never ran.
    let knobless = scenario("consume_batch", "batch", 64, 1, 1_000.0)
        .replace(r#""max_batch_size": 500, "max_batch_age_ms": 200,"#, "");
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{knobless}], "failures": [], "unsupported": []
        }}"#
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("missing its batch knobs"), "{what}");
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }

    let knobbed = scenario("consume_parallel", "parallel", 64, 1, 1_000.0).replace(
        r#""handler_cost""#,
        r#""max_batch_size": 500, "max_batch_age_ms": 200, "handler_cost""#,
    );
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{knobbed}], "failures": [], "unsupported": []
        }}"#
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("non-batch row carries batch knobs"), "{what}");
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

// ── Failed cells are named, never silently absent ────────────────────────────

#[test]
fn a_failed_cell_in_a_slice_is_named_in_the_caption() {
    // A failed cell is absent from results[]; without the note the line just
    // looks like a smaller sweep.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}],
          "failures": [{{
            "flow": "consume_parallel", "mode": "parallel", "payload_bytes": 64,
            "tier": "moderate", "messages": 150000, "consumers": 4,
            "handler": "zero (no-op)", "error": "timeout after 60s"
          }}],
          "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 9_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    assert!(
        svg.contains("kafka: 1 cell(s) in this slice failed to run"),
        "a failed cell must be named in the caption"
    );
    assert!(
        svg.contains("absent, not zero"),
        "the caption must say how to read the absence"
    );
}

// ── Family 3 slices plain consume flows at their own worker counts ──────────

#[test]
fn a_consumer_group_row_never_feeds_the_parallel_bar() {
    // consumer_group rows share mode "parallel", but a coordinated group is a
    // different subscription topology — its (often higher) throughput must
    // not stand in for plain consume.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{},{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 30_000.0),
        scenario("consumer_group", "parallel", 64, 1, 7_654_321.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    for magnitude in ["7654321", "7.7M", "7.65M", "8.6M"] {
        assert!(
            !svg.contains(magnitude),
            "the consumer_group magnitude reached the ordering chart: {magnitude}"
        );
    }
}

#[test]
fn the_fifo_bar_uses_the_mode_pinned_worker_count() {
    // The fifo driver pins its workers to the shard count, so its rows carry
    // consumers=8 even in a 1-consumer sweep. Requiring the global slice
    // count would erase the sequenced bar — the one this chart exists for.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{},{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 30_000.0),
        scenario("consume_fifo", "fifo", 64, 8, 4_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    assert!(
        svg.contains("measured at 8 workers"),
        "a non-1 worker count must be stated on the chart"
    );
    assert!(
        !svg.contains("kafka / sequenced (fifo): no measurement"),
        "the fifo bar was dropped despite being measured"
    );
}

// ── The caption block cannot swallow the chart ──────────────────────────────

#[test]
fn a_pathological_caption_block_is_a_loud_error_not_a_garbage_chart() {
    // Enough long notes would push the footer over the title band and leave
    // the plot a negative-height region rendered as garbage.
    let filler = "x".repeat(400);
    let runs: Vec<String> = (0..8)
        .map(|i| {
            format!(
                r#"{{
                  "backend": "backend{i}", "representative": true,
                  "results": [], "failures": [],
                  "unsupported": [
                    {{ "flow": "publish_single", "reason": "{filler}-a{i}" }},
                    {{ "flow": "publish_batch", "reason": "{filler}-b{i}" }},
                    {{ "flow": "consume_parallel", "reason": "{filler}-c{i}" }},
                    {{ "flow": "consume_fifo", "reason": "{filler}-d{i}" }},
                    {{ "flow": "consume_batch", "reason": "{filler}-e{i}" }},
                    {{ "flow": "consumer_group", "reason": "{filler}-f{i}" }},
                    {{ "flow": "supervisor", "reason": "{filler}-g{i}" }},
                    {{ "flow": "broadcast", "reason": "{filler}-h{i}" }},
                    {{ "flow": "dlq_drain", "reason": "{filler}-i{i}" }}
                  ]
                }}"#
            )
        })
        .collect();
    let doc = parse(&document(&format!(
        "{},{}",
        inmemory_run(true),
        runs.join(",")
    )));
    match chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light) {
        Err(ChartError::Render(msg)) => {
            assert!(msg.contains("leaves no room"), "wrong error: {msg}")
        }
        Ok(_) => panic!("a caption block taller than the canvas rendered a chart"),
        other => panic!("expected a Render refusal, got {other:?}"),
    }
}

// ── One palette, enough colours ──────────────────────────────────────────────

#[test]
fn the_shared_palette_covers_every_fixed_series_list() {
    // Series colours index into each mode's fixed palette; if a family's
    // series list outgrows it, palette_colour wraps deterministically rather
    // than silently reusing the first hue — but the fixed lists should
    // simply fit, with two slots spare for backends this file predates.
    for mode in Mode::ALL {
        assert!(mode.series().len() >= 3, "families 3/4 use three series");
        assert!(
            mode.series().len() >= 6,
            "the backend legend uses six colours"
        );
        assert_eq!(
            mode.series().len(),
            8,
            "two overflow slots beyond the six fixed backends"
        );
    }
}

// ── Round-3 guards: duplicates, provenance, ordering, partial writes ────────

#[test]
fn a_duplicate_backend_key_is_rejected() {
    // Two runs sharing a key would overwrite each other in the line families
    // and render side by side in the bar families — one document, mutually
    // contradictory charts.
    let dup = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 5_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), dup)));
    match chartgen::validate(&doc) {
        Err(ChartError::DuplicateBackendRun { backend }) => assert_eq!(backend, "inmemory"),
        other => panic!("expected DuplicateBackendRun, got {other:?}"),
    }
}

#[test]
fn an_empty_run_with_recorded_failures_is_not_silent() {
    // A run that measured nothing but recorded why is a loud failure, not a
    // silent one: rule 5 must not refuse the harness's own legitimate output
    // for a sweep where every attempted cell errored.
    let failed = r#"{
      "backend": "kafka", "representative": true,
      "results": [],
      "failures": [{
        "flow": "consume_parallel", "mode": "parallel", "payload_bytes": 64,
        "tier": "moderate", "messages": 150000, "consumers": 1,
        "handler": "zero (no-op)", "error": "broker never became ready"
      }],
      "unsupported": []
    }"#;
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), failed)));
    chartgen::validate(&doc).expect("a loudly-failed run must validate");
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    assert!(
        svg.contains("kafka: 1 cell(s) in this slice failed to run"),
        "the failed cell must surface in the caption"
    );
}

#[test]
fn empty_provenance_fields_are_rejected() {
    // A blank generated_at would render 'Generated  — shove …' — an undated
    // chart, the exact problem the provenance rule exists to fix.
    let raw = document(&inmemory_run(true)).replace(
        r#""generated_at": "2026-08-31T22:10:30Z","#,
        r#""generated_at": " ","#,
    );
    match chartgen::validate(&parse(&raw)) {
        Err(ChartError::MissingProvenance { what }) => assert_eq!(what, "generated_at"),
        other => panic!("expected MissingProvenance, got {other:?}"),
    }
}

#[test]
fn the_latency_family_publishes_a_refusal_panel_not_bars() {
    // The v4 dispatch percentiles are queue residency under a saturated
    // drain (and not even that on every backend); publishing them as
    // comparable per-backend latency bars would be a false chart. The family
    // renders a provenanced refusal panel instead until the harness measures
    // under matched load.
    let svg = chartgen::render_to_string(
        &parse(&document(&inmemory_run(true))),
        Family::DispatchLatency,
        Mode::Light,
    )
    .expect("the panel should render");
    assert!(
        svg.contains("no publishable dispatch-latency measurement"),
        "the panel must state the refusal on its face"
    );
    assert!(
        svg.contains("queue"),
        "the caption must say what the field actually measures"
    );
    // The only rects a refusal panel may carry are the painted surface and
    // the border ring — both canvas-sized. Any other rect is a bar drawn
    // from the unpublishable field.
    let (full_size, total) = full_size_rects(&svg);
    assert_eq!(
        full_size, total,
        "no bars may be drawn from the unpublishable field"
    );
}

#[test]
fn a_negative_throughput_row_is_rejected() {
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, -1200.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("throughput"), "wrong invariant named: {what}")
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

#[test]
fn generate_writes_nothing_when_a_late_family_refuses() {
    // Render-stage refusals (here: no in-process run) fire after validate()
    // passes; a partial write would leave the chart directory a mix of fresh
    // and stale files — worse than none.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 5_000.0)
    );
    let doc = parse(&document(&run));
    let out = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("chartgen-partial-{}", std::process::id()));
    std::fs::create_dir_all(&out).expect("create out dir");
    match chartgen::generate(&doc, &out) {
        Err(ChartError::MissingInProcessRun) => {}
        other => panic!("expected MissingInProcessRun, got {other:?}"),
    }
    let leftovers: Vec<_> = std::fs::read_dir(&out)
        .expect("read out dir")
        .filter_map(Result::ok)
        .map(|e| e.file_name())
        .collect();
    std::fs::remove_dir_all(&out).ok();
    assert!(
        leftovers.is_empty(),
        "generate() left partial output behind: {leftovers:?}"
    );
}

#[test]
fn a_single_series_shape_only_chart_still_shows_the_shape() {
    // Family 5 has one bar per group; scaling each shape-only group to its
    // own peak would render every bar at full height — a flat line
    // regardless of the data. The shape must survive across groups.
    let rows = format!(
        "{},{}",
        scenario("publish_single", "parallel", 64, 1, 1_000_000.0),
        scenario("publish_batch", "batch", 64, 1, 10_000.0)
    );
    let run = format!(
        r#"{{
          "backend": "inmemory",
          "broker": {{ "name": "in-process", "version": "n/a", "deployment": "in-process" }},
          "representative": false,
          "results": [{rows}], "failures": [], "unsupported": []
        }}"#
    );
    let svg = chartgen::render_to_string(
        &parse(&document(&run)),
        Family::FrameworkOverhead,
        Mode::Light,
    )
    .expect("chart should render");
    let heights: std::collections::BTreeSet<String> = svg
        .lines()
        .filter(|l| l.contains("<rect") && l.contains("opacity"))
        .filter_map(|l| {
            l.split("height=\"")
                .nth(1)
                .and_then(|r| r.split('\"').next())
                .map(str::to_string)
        })
        .collect();
    assert!(
        heights.len() >= 2,
        "the two bars (100x apart in ns/msg) rendered at identical heights: {heights:?}"
    );
}

#[test]
fn the_legend_never_leaves_the_canvas() {
    // The widest legend this document shape can still produce: the five
    // remaining fixed backends as shape-only runs (each label carries the
    // "(shape only)" suffix) plus both overflow slots taken by long-named
    // unknown backends — a third unknown is refused, so this IS the worst
    // case. Every entry must stay inside the 960px viewBox or a plotted
    // series has no legible label.
    let backends = ["kafka", "nats", "rabbitmq", "redis", "sqs"];
    let mut runs: Vec<String> = backends
        .iter()
        .map(|b| {
            format!(
                r#"{{
                  "backend": "{b}", "representative": false,
                  "broker": {{ "name": "LocalStack", "version": "3", "deployment": "docker" }},
                  "results": [{}], "failures": [], "unsupported": []
                }}"#,
                scenario("consume_parallel", "parallel", 64, 1, 5_000.0)
            )
        })
        .collect();
    for b in ["charlie-backend-name", "foxtrot-backend-name"] {
        runs.push(format!(
            r#"{{
              "backend": "{b}", "representative": false,
              "broker": {{ "name": "LocalStack", "version": "3", "deployment": "docker" }},
              "results": [{}], "failures": [], "unsupported": []
            }}"#,
            scenario("consume_parallel", "parallel", 64, 1, 5_000.0)
        ));
    }
    let doc = parse(&document(&format!(
        "{},{}",
        inmemory_run(true),
        runs.join(",")
    )));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    for (x, _, _, content) in texts(&svg) {
        assert!(
            x < 960.0,
            "a text element starts beyond the canvas edge (x={x}): {content}"
        );
    }
}

#[test]
fn an_unbreakable_token_in_a_reason_is_split_not_overflowed() {
    let url = format!("https://example.com/{}", "a".repeat(200));
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [],
          "unsupported": [{{ "flow": "consume_fifo", "reason": "tracked at {url}" }}]
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 5_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    for (_, _, _, content) in texts(&svg) {
        assert!(
            content.chars().count() <= 120,
            "a caption line was not wrapped ({} chars): {content}",
            content.chars().count()
        );
    }
}

// ── Round-5 guards ───────────────────────────────────────────────────────────

#[test]
fn the_document_level_failure_count_reaches_every_caption() {
    // Failures in flows no family charts (the committed broadcast timeouts)
    // must not be invisible on every published artifact.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}],
          "failures": [{{
            "flow": "broadcast", "mode": "parallel", "payload_bytes": 65536,
            "tier": "moderate", "messages": 5000, "consumers": 8,
            "handler": "zero (no-op)", "error": "timeout after 60s"
          }}],
          "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 9_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    for (family, svg) in render_all(&doc) {
        assert!(
            svg.contains("1 recorded failure(s) in the dataset"),
            "{family:?} hides the document-level failure count"
        );
    }
}

#[test]
fn an_unsupported_entry_without_a_reason_is_rejected() {
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [],
          "unsupported": [{{ "flow": "consume_batch", "reason": "  " }}]
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 9_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("without a reason"), "{what}")
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

#[test]
fn a_flow_both_measured_and_declared_unsupported_is_rejected() {
    // A document contradicting itself would render the bar while the
    // declaration silently vanished.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [],
          "unsupported": [{{ "flow": "consume_parallel", "reason": "declared and measured" }}]
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 9_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("carries measured rows"), "{what}")
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

#[test]
fn a_line_never_bridges_a_category_with_no_publishable_value() {
    // kafka has framework rows at 64 B and 64 KiB and a setup-bound 1 KiB
    // between them: a single polyline would assert an interpolated value at
    // exactly the withheld category.
    let rows = format!(
        "{},{},{}",
        scenario("consume_parallel", "parallel", 64, 1, 50_000.0),
        scenario_with_window(
            "consume_parallel",
            "parallel",
            1024,
            1,
            57_800.0,
            "setup_bound",
            0.57
        ),
        scenario("consume_parallel", "parallel", 65536, 1, 8_000.0)
    );
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{rows}], "failures": [], "unsupported": []
        }}"#
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsPayload, Mode::Light)
        .expect("chart should render");
    // Three categories sit ~316px apart; a segment bridging the withheld
    // middle category spans two steps (~630px). No polyline segment may span
    // more than one category step.
    for chunk in svg.split("<polyline").skip(1) {
        let Some(points) = chunk
            .split("points=\"")
            .nth(1)
            .and_then(|r| r.split('\"').next())
        else {
            continue;
        };
        let xs: Vec<f64> = points
            .split_whitespace()
            .filter_map(|p| p.split(',').next())
            .filter_map(|x| x.parse().ok())
            .collect();
        for pair in xs.windows(2) {
            assert!(
                (pair[1] - pair[0]).abs() < 400.0,
                "a polyline segment bridges a withheld category: {points}"
            );
        }
    }
    assert!(
        svg.contains("kafka: partial") && svg.contains("window under 1 s") && svg.contains("1 KiB"),
        "the withheld cell must still be named in the caption"
    );
}

#[test]
fn a_subnormal_throughput_cannot_render_a_corrupt_overhead_chart() {
    // 1e9 / 1e-300 = +inf: an infinite axis maps every coordinate to NaN and
    // plotters completes the render — a clean-exiting corrupt SVG.
    let raw = document(&inmemory_run(true)).replace(
        r#""throughput_msg_per_sec": 50000"#,
        r#""throughput_msg_per_sec": 1e-300"#,
    );
    let doc = parse(&raw);
    if chartgen::validate(&doc).is_err() {
        // Fine too — rejected before any render.
        return;
    }
    for family in Family::ALL {
        if let Ok(svg) = chartgen::render_to_string(&doc, family, Mode::Light) {
            assert!(
                !svg.contains("NaN"),
                "{family:?} rendered NaN coordinates from a subnormal throughput"
            );
        }
    }
}

// ── Round-6 guards ───────────────────────────────────────────────────────────

#[test]
fn a_zero_batch_knob_is_rejected() {
    // The writer's builders assert > 0, so a zero knob is a corrupt row that
    // would caption "up to 0 messages per batch".
    let zeroed = scenario("consume_batch", "batch", 64, 1, 1_000.0)
        .replace(r#""max_batch_size": 500"#, r#""max_batch_size": 0"#);
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{zeroed}], "failures": [], "unsupported": []
        }}"#
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("batch knob is zero"), "{what}")
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

#[test]
fn a_framework_row_without_a_setup_window_is_rejected() {
    // The barrier always records what it excluded; framework with no
    // setup_secs is a row the harness cannot produce.
    let raw = document(&inmemory_run(true)).replace(r#""setup_secs": 0.4,"#, "");
    let doc = parse(&raw);
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("no recorded setup window"), "{what}")
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

#[test]
fn a_shape_only_group_cannot_launder_a_non_finite_bar() {
    // The finiteness guard must cover shape-only groups too: their values
    // feed the scaling peak, and inf/inf renders NaN geometry.
    let raw = document(&inmemory_run(false)).replace(
        r#""throughput_msg_per_sec": 50000"#,
        r#""throughput_msg_per_sec": 1e-300"#,
    );
    let doc = parse(&raw);
    if chartgen::validate(&doc).is_err() {
        return;
    }
    for family in Family::ALL {
        if let Ok(svg) = chartgen::render_to_string(&doc, family, Mode::Light) {
            assert!(
                !svg.contains("NaN"),
                "{family:?} rendered NaN coordinates from a subnormal throughput"
            );
        }
    }
}

#[test]
fn family3_compares_modes_at_a_shared_worker_count() {
    // parallel measured at 1 and 8 workers, fifo only at 8: the bars must
    // both come from the 8-worker cells — a 1-worker parallel bar against an
    // 8-worker fifo bar inverted the ordering-cost story.
    let rows = format!(
        "{},{},{}",
        scenario("consume_parallel", "parallel", 64, 1, 33_333.0),
        scenario("consume_parallel", "parallel", 64, 8, 88_888.0),
        scenario_with_cost("consume_fifo", "fifo", 64, 8, 44_444.0, "setup_bound")
    );
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{rows}], "failures": [], "unsupported": []
        }}"#
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    assert!(
        !svg.contains("33.3k") && !svg.contains("33333"),
        "the 1-worker parallel magnitude must not be charted when 8 is the shared count"
    );
    assert!(
        svg.contains("measured at 8 workers"),
        "the shared non-1 worker count must be stated"
    );
}

#[test]
fn the_lockfile_carries_no_font_stack() {
    // Byte-determinism rests on plotters resolving without `ttf`: text
    // extents would otherwise come from host fonts and every label
    // coordinate would move between machines. Feature unification (e.g. a
    // criterion bump) could re-enable it without any diff in our Cargo.toml,
    // so pin the resolved graph, not the declaration.
    let lock = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"),
    )
    .expect("read Cargo.lock");
    for font_dep in ["ttf-parser", "font-kit", "ab_glyph", "rusttype"] {
        assert!(
            !lock.contains(&format!("name = \"{font_dep}\"")),
            "Cargo.lock resolves {font_dep}: plotters' ttf feature got re-enabled \
             and rendered text extents are now host-dependent"
        );
    }
}

// ── The committed artifact renders ──────────────────────────────────────────

#[test]
fn the_committed_results_document_renders_every_family() {
    // Guards the pair that ship together: if the committed JSON stops
    // satisfying the contract, this fails here rather than in the CI leg that
    // regenerates the charts.
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("benches/results/bench-results.json");
    let doc = chartgen::load(&path).expect("the committed results document should load");

    for family in Family::ALL {
        for mode in Mode::ALL {
            let svg = chartgen::render_to_string(&doc, family, mode).unwrap_or_else(|e| {
                panic!("{family:?} ({mode:?}) should render from the committed doc: {e}")
            });
            assert!(
                svg.starts_with("<svg"),
                "{family:?} ({mode:?}) did not produce an SVG"
            );
            assert!(
                svg.contains(&doc.generated_at),
                "{family:?} ({mode:?}) lost the provenance date"
            );
            // The committed SVGs must be exactly what this code renders from
            // the committed document — anyone who updates one half without
            // the other fails here, in `cargo test`, without waiting for a
            // CI leg.
            let committed = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("docs/public/bench")
                .join(family.filename(mode));
            let committed = std::fs::read_to_string(&committed)
                .unwrap_or_else(|e| panic!("read {}: {e}", committed.display()));
            assert!(
                svg == committed,
                "{family:?} ({mode:?}): the committed SVG is not what the committed \
                 document renders — regenerate with `cargo run --no-default-features \
                 --example chartgen -- --input benches/results/bench-results.json \
                 --out-dir docs/public/bench`"
            );
        }
    }
}

// ── Rule 7: a rate needs a window long enough to be one ─────────────────────

/// An in-process run whose publish rows drained in a few milliseconds — the
/// shape of the committed dataset, where 5,000 messages through an in-process
/// publisher took 0.008 s.
fn inmemory_run_with_short_publish_windows() -> String {
    let mut rows = vec![scenario("consume_parallel", "parallel", 64, 1, 100_000.0)];
    for (flow, mode) in [("publish_single", "parallel"), ("publish_batch", "batch")] {
        rows.push(scenario_with_window(
            flow,
            mode,
            64,
            1,
            631_384.0,
            "no_handler",
            0.0079,
        ));
    }
    format!(
        r#"{{
          "backend": "inmemory",
          "broker": {{ "name": "in-process", "version": "n/a", "deployment": "in-process" }},
          "representative": true,
          "results": [{}],
          "failures": [],
          "unsupported": []
        }}"#,
        rows.join(",")
    )
}

#[test]
fn a_publish_row_with_a_sub_second_window_never_becomes_an_overhead_bar() {
    // 631,384 msg/s over 0.008 s is 1,584 ns/msg — a number, but not a rate:
    // the window is shorter than the scheduler's own noise. The harness holds
    // consume rows to a one-second floor before it will call them a
    // framework measurement; the publish rows have no such floor, so the
    // chart has to hold them to it instead of publishing the reciprocal.
    let doc = parse(&document(&inmemory_run_with_short_publish_windows()));
    let svg = chartgen::render_to_string(&doc, Family::FrameworkOverhead, Mode::Light)
        .expect("chart should render");

    for magnitude in ["1584", "1.6k", "1,584"] {
        assert!(
            !svg.contains(magnitude),
            "the overhead chart published {magnitude} ns/msg from a 0.008 s window"
        );
    }
    assert!(
        svg.contains("publish_single") && svg.contains("publish_batch"),
        "the withheld flows must still be named"
    );
    assert!(
        svg.contains("window under 1 s"),
        "the caption must say the window was too short to publish a rate"
    );
    // The withheld flows are not columns: a column with no bar and no n/s
    // marker would read as a zero.
    let columns = texts(&svg)
        .into_iter()
        .filter(|(_, _, _, t)| t == "publish_single" || t == "publish_batch")
        .count();
    assert_eq!(columns, 0, "a withheld flow must not open an axis column");
}

#[test]
fn a_short_window_lower_bound_is_not_drawn() {
    // A setup-bound fifo row from a 0.18 s window: the bound is technically
    // true, but a value read off a window that short is noise, and drawn at
    // 219k msg/s it scaled the whole axis so every real bar became a sliver.
    let mut run = inmemory_run(true);
    run = run.replace(
        &scenario("consume_fifo", "fifo", 64, 1, 4_000.0),
        &scenario_with_window(
            "consume_fifo",
            "fifo",
            64,
            1,
            219_087.0,
            "setup_bound",
            0.18,
        ),
    );
    let doc = parse(&document(&run));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");

    for magnitude in ["219087", "219k", "200k", "250k"] {
        assert!(
            !svg.contains(magnitude),
            "the ordering chart drew or scaled to a lower bound from a 0.18 s window ({magnitude})"
        );
    }
    assert!(
        svg.contains("n/s"),
        "the withheld fifo bar must leave an explicit marker, not a blank slot"
    );
    assert!(
        svg.contains("sequenced (fifo)") && svg.contains("window under 1 s"),
        "the caption must say why the fifo bar is withheld"
    );
    assert!(
        !svg.contains("lower bound (muted bar)"),
        "no lower-bound bar exists, so the lower-bound caption must not appear"
    );
}

#[test]
fn a_framework_row_with_a_sub_second_window_is_refused() {
    // The harness never stamps `framework` on a window under its own floor,
    // so a row like this is a hand-edit or a producer regression — and it is
    // exactly the number the marker system exists to keep off an absolute
    // axis.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario_with_window(
            "consume_parallel",
            "parallel",
            64,
            1,
            57_853.0,
            "framework",
            0.57
        )
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow {
            backend,
            flow,
            what,
        }) => {
            assert_eq!(backend, "kafka");
            assert_eq!(flow, "consume_parallel");
            assert!(what.contains("window"), "wrong reason: {what}");
        }
        other => panic!("expected a MalformedRow refusal, got {other:?}"),
    }
    for family in Family::ALL {
        assert!(
            chartgen::render_to_string(&doc, family, Mode::Light).is_err(),
            "{family:?} rendered a document with a sub-second framework row"
        );
    }
}

#[test]
fn a_row_without_a_measured_window_is_refused() {
    // Every publishability decision now also asks how long the window was; a
    // row that cannot answer cannot be charted, and skipping it would be the
    // silent omission rule 3 forbids.
    for duration in ["0.0", "-1.0", "null"] {
        let row = scenario("consume_parallel", "parallel", 64, 1, 50_000.0).replace(
            "\"duration_secs\": 2.0",
            &format!("\"duration_secs\": {duration}"),
        );
        let run = format!(
            r#"{{
              "backend": "kafka", "representative": true,
              "results": [{row}], "failures": [], "unsupported": []
            }}"#
        );
        let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
        match chartgen::validate(&doc) {
            Err(ChartError::MalformedRow { what, .. }) => {
                assert!(what.contains("duration_secs"), "wrong reason: {what}")
            }
            other => panic!("duration_secs={duration}: expected MalformedRow, got {other:?}"),
        }
    }
}

// ── `supervisor` is a spelling, not a gap ───────────────────────────────────

#[test]
fn the_supervisor_column_is_captioned_as_an_alias_not_a_gap() {
    // The harness assigns the `supervisor` name to SQS and `consume_parallel`
    // to every other backend for the same `run` primitive, and the in-process
    // driver never emits a supervisor row. Captioning that as a gap says a
    // measurement was skipped when nothing was.
    let doc = parse(&document(&inmemory_run(true)));
    let svg = chartgen::render_to_string(&doc, Family::FrameworkOverhead, Mode::Light)
        .expect("chart should render");
    assert!(
        !svg.contains("capability hole): supervisor"),
        "supervisor must not be captioned as an unmeasured gap"
    );
    assert!(
        svg.contains("supervisor") && svg.contains("spelling of consume_parallel"),
        "the caption must explain that supervisor is the SQS spelling of consume_parallel"
    );
}

// ── The caption block may not squash the plot ───────────────────────────────

#[test]
fn identical_explanations_across_modes_collapse_to_one_caption_line() {
    // An SQS run declares the same reason for `consume_parallel` and
    // `consume_fifo`; two three-line captions saying the same thing is what
    // pushed the committed ordering chart's plot body down to ~50px.
    let reason = "not measured in this document: SQS runs only against LocalStack and requires \
                  a LocalStack Pro auth token, which is unavailable in the environment that \
                  produced these results";
    let run = format!(
        r#"{{
          "backend": "sqs", "representative": false,
          "results": [], "failures": [],
          "unsupported": [
            {{ "flow": "publish_single", "reason": "{reason}" }},
            {{ "flow": "publish_batch", "reason": "{reason}" }},
            {{ "flow": "consume_parallel", "reason": "{reason}" }},
            {{ "flow": "consume_fifo", "reason": "{reason}" }},
            {{ "flow": "consume_batch", "reason": "{reason}" }},
            {{ "flow": "consumer_group", "reason": "{reason}" }},
            {{ "flow": "supervisor", "reason": "{reason}" }},
            {{ "flow": "broadcast", "reason": "{reason}" }},
            {{ "flow": "dlq_drain", "reason": "{reason}" }}
          ]
        }}"#
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    assert_eq!(
        svg.matches("LocalStack Pro auth token").count(),
        1,
        "the shared reason must be captioned once, naming both modes"
    );
    assert!(
        svg.contains("sqs / parallel, sequenced (fifo), batch:"),
        "the merged caption must name every mode it covers"
    );
}

#[test]
fn the_plot_body_keeps_a_minimum_height_under_a_tall_caption() {
    // The frame refuses a caption block that leaves the plot under a
    // readable height, rather than rendering bars a few pixels tall. The
    // committed ordering chart carried nineteen caption lines and a ~50px
    // plot; that is a garbage chart, not a chart with a long caption.
    let filler = "x".repeat(120);
    let runs: Vec<String> = (0..4)
        .map(|i| {
            format!(
                r#"{{
                  "backend": "backend{i}", "representative": true,
                  "results": [{}], "failures": [],
                  "unsupported": [
                    {{ "flow": "consume_parallel", "reason": "{filler}-a{i}" }},
                    {{ "flow": "consume_fifo", "reason": "{filler}-b{i}" }},
                    {{ "flow": "consume_batch", "reason": "{filler}-c{i}" }}
                  ]
                }}"#,
                scenario("publish_single", "parallel", 64, 1, 10_000.0)
            )
        })
        .collect();
    let doc = parse(&document(&format!(
        "{},{}",
        inmemory_run(true),
        runs.join(",")
    )));
    match chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light) {
        Err(ChartError::Render(msg)) => {
            assert!(msg.contains("leaves no room"), "wrong error: {msg}")
        }
        Ok(svg) => {
            // If it renders, the plot must be tall enough to read: the y-axis
            // tick labels span the plot body.
            let ticks: Vec<f64> = texts(&svg)
                .into_iter()
                .filter(|(x, _, _, t)| *x < Y_TICK_BAND_X && (t.ends_with('k') || t == "0.0"))
                .map(|(_, y, _, _)| y)
                .collect();
            let span = ticks.iter().cloned().fold(0.0, f64::max)
                - ticks.iter().cloned().fold(f64::MAX, f64::min);
            assert!(
                span >= chartgen::MIN_PLOT_PX as f64 * 0.8,
                "the plot body spans only {span:.0}px under a tall caption"
            );
        }
        other => panic!("expected a chart or a Render refusal, got {other:?}"),
    }
}

// ── Review round 1: every absence in a line chart is legible ────────────────

/// A declared, unmeasured backend: every flow in `unsupported[]`, no rows.
fn declared_only_run(backend: &str, reason: &str) -> String {
    let flows = [
        "publish_single",
        "publish_batch",
        "consume_parallel",
        "consume_fifo",
        "consume_batch",
        "consumer_group",
        "supervisor",
        "broadcast",
        "dlq_drain",
    ];
    let declared: Vec<String> = flows
        .iter()
        .map(|f| format!(r#"{{ "flow": "{f}", "reason": "{reason}" }}"#))
        .collect();
    format!(
        r#"{{
          "backend": "{backend}", "representative": false,
          "results": [], "failures": [],
          "unsupported": [{}]
        }}"#,
        declared.join(",")
    )
}

#[test]
fn line_charts_mark_an_unsupported_backend_in_the_legend() {
    // The bar families put an `n/s` marker in the backend's slot; a line
    // chart has no slot, so the legend is where the backend must appear —
    // caption prose alone leaves the plot looking like a four-backend sweep.
    let doc = parse(&document(&format!(
        "{},{}",
        inmemory_run(true),
        declared_only_run("sqs", "not measured in this document")
    )));
    for family in [Family::ThroughputVsConsumers, Family::ThroughputVsPayload] {
        let svg =
            chartgen::render_to_string(&doc, family, Mode::Light).expect("chart should render");
        assert!(
            texts(&svg).iter().any(|(_, _, _, t)| t == "sqs (n/s)"),
            "{family:?}: the unsupported backend has no legend marker"
        );
    }
}

#[test]
fn a_missing_category_in_a_measured_series_is_captioned_as_a_gap() {
    // kafka measured 1 and 4 consumers; inmemory establishes 2 as a category.
    // The kafka line correctly does not bridge 1→4, but without a caption the
    // missing point reads as a smaller sweep — or as a cell that failed.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{},{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 20_000.0),
        scenario("consume_parallel", "parallel", 64, 4, 60_000.0)
    );
    // A second backend establishes the 2-consumer category.
    let full = format!(
        r#"{{
          "backend": "nats", "representative": true,
          "results": [{},{},{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 5_000.0),
        scenario("consume_parallel", "parallel", 64, 2, 9_000.0),
        scenario("consume_parallel", "parallel", 64, 4, 16_000.0)
    );
    let doc = parse(&document(&format!(
        "{},{},{}",
        inmemory_run(true),
        run,
        full
    )));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    // The caption wraps, so assert on the joined text runs.
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        caption.contains("kafka: partial"),
        "a series with a missing category must be captioned partial"
    );
    assert!(
        caption.contains("a gap, not a capability hole") && caption.contains("consumers 2"),
        "the missing category must be named as a gap: {caption}"
    );
}

#[test]
fn a_short_window_setup_bound_cell_is_captioned_as_too_short_in_the_line_charts() {
    // On a barrier-holding flow, `setup_bound` with a recorded setup window
    // means exactly one thing: the drain was under the floor. Captioning it
    // as "window includes coordination cost" names the wrong cause.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{},{},{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 68_000.0),
        scenario_with_window(
            "consume_parallel",
            "parallel",
            1024,
            1,
            57_853.0,
            "setup_bound",
            0.57
        ),
        scenario_with_window(
            "consume_parallel",
            "parallel",
            65536,
            1,
            2_521.0,
            "setup_bound",
            1.98
        )
        .replace("\"setup_secs\": 0.4", "\"setup_secs\": null")
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsPayload, Mode::Light)
        .expect("chart should render");
    assert!(
        svg.contains("kafka: partial"),
        "the kafka series must be partial"
    );
    assert!(
        svg.contains("window under 1 s") && svg.contains("1 KiB"),
        "the sub-second cell must be captioned as too short, by category: {svg}"
    );
    assert!(
        svg.contains("coordination cost") && svg.contains("64 KiB"),
        "the long-window setup-bound cell keeps the coordination-cost caption"
    );
    assert!(!svg.contains("57853") && !svg.contains("57.9k"));
}

#[test]
fn the_latency_panel_accounts_for_every_backend() {
    // The refusal panel withholds every percentile for one shared reason,
    // but a chart that names no backend cannot show that sqs was never
    // measured while the other five were.
    let doc = parse(&document(&format!(
        "{},{}",
        inmemory_run(true),
        declared_only_run("sqs", "not measured in this document")
    )));
    let svg = chartgen::render_to_string(&doc, Family::DispatchLatency, Mode::Light)
        .expect("panel should render");
    assert!(
        svg.contains("inmemory") && svg.contains("withheld"),
        "the panel must name the backends whose percentiles are withheld"
    );
    assert!(
        svg.contains("sqs") && svg.contains("no measured rows"),
        "the panel must distinguish a backend with no rows at all"
    );
}

#[test]
fn empty_provenance_fields_are_refused() {
    // The provenance block is the artifact's reason to exist; a v4 document
    // that omits the toolchain or the host cannot render a complete one.
    let base = document(&inmemory_run(true));
    for (needle, replacement, what) in [
        (
            r#""rust_version": "rustc 1.91.1""#,
            r#""rust_version": """#,
            "rust_version",
        ),
        (r#""cpu": "aarch64""#, r#""cpu": " ""#, "hardware.cpu"),
        (
            r#""os": "Debian GNU/Linux 13""#,
            r#""os": """#,
            "hardware.os",
        ),
    ] {
        assert!(base.contains(needle), "fixture drifted: {needle}");
        let doc = parse(&base.replace(needle, replacement));
        match chartgen::validate(&doc) {
            Err(ChartError::MissingProvenance { what: got }) => assert_eq!(got, what),
            other => panic!("{what}: expected MissingProvenance, got {other:?}"),
        }
    }
}

#[test]
fn a_failure_without_an_error_is_refused() {
    // failures[] is the loud account of what happened; an entry with no
    // diagnostic is a coordinate, not an account — and it must not exempt an
    // empty run from the silent-run rule.
    let run = r#"{
          "backend": "kafka", "representative": true,
          "results": [],
          "failures": [{ "flow": "consume_parallel", "payload_bytes": 64, "consumers": 1,
                         "handler": "zero (no-op)", "error": " " }],
          "unsupported": []
        }"#;
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("error"), "wrong reason: {what}")
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

// ── Review round 2 ──────────────────────────────────────────────────────────

#[test]
fn a_marker_that_contradicts_its_flow_is_refused() {
    // `no_handler` says "no consumer was constructed" — only a publish flow
    // can say that. A consume row wearing it would be published as an
    // absolute cost without the window rule ever seeing it.
    for (flow, mode, cost) in [
        ("consume_parallel", "parallel", "no_handler"),
        ("publish_single", "parallel", "framework"),
        ("publish_batch", "batch", "setup_bound"),
        ("publish_single", "parallel", "handler_bound"),
    ] {
        let run = format!(
            r#"{{
              "backend": "kafka", "representative": true,
              "results": [{}], "failures": [], "unsupported": []
            }}"#,
            scenario_with_cost(flow, mode, 64, 1, 50_000.0, cost)
        );
        let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
        match chartgen::validate(&doc) {
            Err(ChartError::MalformedRow { what, .. }) => {
                assert!(what.contains(cost), "{flow}/{cost}: wrong reason: {what}")
            }
            other => panic!("{flow}/{cost}: expected MalformedRow, got {other:?}"),
        }
    }
}

#[test]
fn a_category_no_backend_can_publish_still_reaches_the_axis_and_caption() {
    // Every backend's 64 KiB cell is setup-bound. The category must still be
    // on the axis and every backend's caption must name it; dropping the
    // column reads as a two-payload sweep.
    let rows = format!(
        "{},{},{}",
        scenario("consume_parallel", "parallel", 64, 1, 60_000.0),
        scenario("consume_parallel", "parallel", 1024, 1, 40_000.0),
        scenario_with_window(
            "consume_parallel",
            "parallel",
            65536,
            1,
            3_000.0,
            "setup_bound",
            0.5
        )
    );
    let a = format!(
        r#"{{ "backend": "kafka", "representative": true, "results": [{rows}], "failures": [], "unsupported": [] }}"#
    );
    let b = format!(
        r#"{{ "backend": "nats", "representative": true, "results": [{rows}], "failures": [], "unsupported": [] }}"#
    );
    let doc = parse(&document(&format!("{a},{b}")));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsPayload, Mode::Light)
        .expect("chart should render");
    let labels: Vec<String> = texts(&svg).into_iter().map(|(_, _, _, t)| t).collect();
    assert!(
        labels.iter().any(|t| t == "64 KiB"),
        "the unpublishable category left the axis"
    );
    let caption = labels.join(" ");
    assert!(
        caption.contains("kafka: partial") && caption.contains("nats: partial"),
        "every backend must account for the withheld category: {caption}"
    );
}

#[test]
fn a_partial_shape_only_series_keeps_its_withheld_accounting() {
    // A non-representative series that measured two of three categories:
    // the shape-only caveat must not swallow the reason the third is absent.
    let run = format!(
        r#"{{
          "backend": "sqs", "representative": false,
          "broker": {{ "name": "LocalStack", "version": "x", "deployment": "localstack" }},
          "results": [{},{},{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 900.0),
        scenario("consume_parallel", "parallel", 1024, 1, 800.0),
        scenario_with_window(
            "consume_parallel",
            "parallel",
            65536,
            1,
            700.0,
            "setup_bound",
            0.4
        )
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsPayload, Mode::Light)
        .expect("chart should render");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(caption.contains("sqs: shape only"), "{caption}");
    assert!(
        caption.contains("window under 1 s") && caption.contains("64 KiB"),
        "the shape-only series lost its withheld cell: {caption}"
    );
}

#[test]
fn a_sleeping_handler_cell_is_captioned_as_measured_not_as_a_gap() {
    // A handler_bound row is a real measurement that the charts refuse on
    // purpose; "no measurement" is the wrong word for it in both bar families.
    let mut rows = vec![scenario_with_cost(
        "consume_parallel",
        "parallel",
        64,
        1,
        470.0,
        "handler_bound",
    )];
    rows.push(scenario("publish_single", "parallel", 64, 1, 50_000.0));
    let run = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "broker": {{ "name": "in-process", "version": "n/a", "deployment": "in-process" }},
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows.join(",")
    );
    let doc = parse(&document(&run));
    for family in [Family::ParallelVsSequenced, Family::FrameworkOverhead] {
        let svg =
            chartgen::render_to_string(&doc, family, Mode::Light).expect("chart should render");
        let caption = texts(&svg)
            .into_iter()
            .map(|(_, _, _, t)| t)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            caption.contains("simulated sleep"),
            "{family:?}: the sleeping-handler cell must be captioned as measured but withheld: {caption}"
        );
        assert!(
            !caption.contains("a gap, not a capability hole): consume_parallel")
                && !caption.contains("inmemory / parallel: no measurement"),
            "{family:?}: a measured cell was captioned as a gap: {caption}"
        );
    }
}

#[test]
fn an_axis_peak_that_cannot_take_headroom_is_refused() {
    // Headroom is a multiplication; near f64::MAX it overflows to infinity
    // and plotters maps every coordinate to NaN in a clean-looking file.
    let huge = format!("{:e}", f64::MAX / 1.05);
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 1.0).replace(
            "\"throughput_msg_per_sec\": 1,",
            &format!("\"throughput_msg_per_sec\": {huge},")
        )
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    for family in [Family::ThroughputVsConsumers, Family::ParallelVsSequenced] {
        match chartgen::render_to_string(&doc, family, Mode::Light) {
            Err(ChartError::Render(msg)) => assert!(msg.contains("headroom"), "{msg}"),
            other => panic!("{family:?}: expected a Render refusal, got {other:?}"),
        }
    }
}

// ── Review round 3 ──────────────────────────────────────────────────────────

#[test]
fn a_sleeping_handler_row_stamped_as_shove_cost_is_refused() {
    // The marker is derived from the handler profile; a heavy handler whose
    // row claims `framework` would be published as shove's own cost.
    for (handler, cost) in [
        ("heavy (1-5s)", "framework"),
        ("slow (50-300ms)", "setup_bound"),
        ("zero (no-op)", "handler_bound"),
        ("fast (1-5ms)", "handler_amortised"),
    ] {
        let row = scenario_with_cost("consume_parallel", "parallel", 64, 1, 470.0, cost)
            .replace(
                "\"handler\": \"zero (no-op)\"",
                &format!("\"handler\": \"{handler}\""),
            )
            .replace(
                "\"handler\": \"slow (50-300ms)\"",
                &format!("\"handler\": \"{handler}\""),
            );
        let run = format!(
            r#"{{
              "backend": "kafka", "representative": true,
              "results": [{row}], "failures": [], "unsupported": []
            }}"#
        );
        let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
        match chartgen::validate(&doc) {
            Err(ChartError::MalformedRow { what, .. }) => {
                assert!(
                    what.contains(cost),
                    "{handler}/{cost}: wrong reason: {what}"
                )
            }
            other => panic!("{handler}/{cost}: expected MalformedRow, got {other:?}"),
        }
    }
}

#[test]
fn a_flow_with_the_wrong_mode_is_refused() {
    // Family 3 places bars by `mode`; a `consume_batch` row saying
    // `parallel` would publish a batch number as the parallel bar.
    for (flow, mode, cost) in [
        ("consume_batch", "parallel", "framework"),
        ("consume_fifo", "parallel", "setup_bound"),
        ("consume_parallel", "batch", "framework"),
        ("publish_batch", "parallel", "no_handler"),
    ] {
        let run = format!(
            r#"{{
              "backend": "kafka", "representative": true,
              "results": [{}], "failures": [], "unsupported": []
            }}"#,
            scenario_with_cost(flow, mode, 64, 1, 50_000.0, cost)
        );
        let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
        match chartgen::validate(&doc) {
            Err(ChartError::MalformedRow { what, .. }) => {
                assert!(what.contains("mode"), "{flow}/{mode}: wrong reason: {what}")
            }
            other => panic!("{flow}/{mode}: expected MalformedRow, got {other:?}"),
        }
    }
}

#[test]
fn a_failed_bar_cell_is_captioned_as_failed_not_as_a_gap() {
    // A cell in failures[] is an absence with a recorded cause; the bar
    // families must not also call it a gap.
    let failure = |flow: &str| {
        format!(
            r#"{{ "flow": "{flow}", "payload_bytes": 64, "consumers": 1, "tier": "moderate",
                 "handler": "zero (no-op)", "error": "timeout after 60s" }}"#
        )
    };
    let kafka = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [{},{}], "unsupported": []
        }}"#,
        scenario("publish_single", "parallel", 64, 1, 50_000.0),
        failure("consume_parallel"),
        failure("consume_fifo")
    );
    let inmemory = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "broker": {{ "name": "in-process", "version": "n/a", "deployment": "in-process" }},
          "results": [{}], "failures": [{}], "unsupported": []
        }}"#,
        scenario("consumer_group", "parallel", 64, 1, 50_000.0),
        failure("consume_parallel")
    );
    let doc = parse(&document(&format!("{inmemory},{kafka}")));
    for family in [Family::ParallelVsSequenced, Family::FrameworkOverhead] {
        let svg =
            chartgen::render_to_string(&doc, family, Mode::Light).expect("chart should render");
        let caption = texts(&svg)
            .into_iter()
            .map(|(_, _, _, t)| t)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(caption.contains("failed"), "{family:?}: {caption}");
        assert!(
            !caption.contains("kafka / parallel: no measurement")
                && !caption.contains("kafka / parallel, sequenced (fifo): no measurement")
                && !caption.contains("capability hole): consume_parallel"),
            "{family:?}: a failed cell was captioned as a gap: {caption}"
        );
    }
}

#[test]
fn a_sleeping_handler_only_line_slice_is_not_labelled_setup_bound() {
    let row = scenario_with_cost(
        "consume_parallel",
        "parallel",
        64,
        1,
        470.0,
        "handler_bound",
    )
    .replace(
        "\"handler\": \"zero (no-op)\"",
        "\"handler\": \"slow (50-300ms)\"",
    );
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{row}], "failures": [], "unsupported": []
        }}"#
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        !caption.contains("kafka: setup-bound"),
        "a sleeping-handler slice was labelled setup-bound: {caption}"
    );
    assert!(caption.contains("simulated sleep"), "{caption}");
}

// ── Review round 4 ──────────────────────────────────────────────────────────

#[test]
fn a_barrier_less_flow_cannot_carry_the_framework_marker() {
    // `consume_fifo` and `dlq_drain` hold no readiness barrier, so the
    // harness can never certify their window as a drain; a `framework` row
    // for either would reach the ordering chart as an absolute bar.
    for (flow, mode) in [("consume_fifo", "fifo"), ("dlq_drain", "parallel")] {
        let run = format!(
            r#"{{
              "backend": "kafka", "representative": true,
              "results": [{}], "failures": [], "unsupported": []
            }}"#,
            scenario_with_cost(flow, mode, 64, 1, 50_000.0, "framework")
        );
        let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
        match chartgen::validate(&doc) {
            Err(ChartError::MalformedRow { what, .. }) => {
                assert!(what.contains("barrier"), "{flow}: wrong reason: {what}")
            }
            other => panic!("{flow}: expected MalformedRow, got {other:?}"),
        }
    }
}

#[test]
fn the_overhead_chart_discloses_short_windows_behind_setup_bound_flows() {
    // The committed in-process fifo row is setup-bound *and* 0.18 s long;
    // "coordination cost" alone hides the second reason it cannot be a rate.
    let mut run = inmemory_run(true);
    run = run.replace(
        &scenario("consume_fifo", "fifo", 64, 1, 4_000.0),
        &scenario_with_window(
            "consume_fifo",
            "fifo",
            64,
            1,
            219_087.0,
            "setup_bound",
            0.18,
        ),
    );
    let doc = parse(&document(&run));
    let svg = chartgen::render_to_string(&doc, Family::FrameworkOverhead, Mode::Light)
        .expect("chart should render");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        caption.contains("consume_fifo (0.180 s)"),
        "the short fifo window must be disclosed with its length: {caption}"
    );
    assert!(
        !caption.contains("no framework number published): consume_fifo"),
        "a sub-second fifo row must not be filed under coordination cost alone: {caption}"
    );
}

#[test]
fn an_sqs_supervisor_row_is_charted_as_the_parallel_consume_it_spells() {
    // `supervisor` is the SQS name for the parallel-consume primitive; a
    // measured SQS run must appear in the consume slices (shape-only, since
    // LocalStack is not representative), not be captioned as a gap.
    let run = format!(
        r#"{{
          "backend": "sqs", "representative": false,
          "broker": {{ "name": "LocalStack", "version": "x", "deployment": "localstack" }},
          "results": [{},{}], "failures": [],
          "unsupported": [
            {{ "flow": "consume_parallel", "reason": "spelled supervisor on this backend" }},
            {{ "flow": "consume_fifo", "reason": "no fifo on sqs in this harness" }},
            {{ "flow": "consume_batch", "reason": "no batch on sqs" }}
          ]
        }}"#,
        scenario("supervisor", "parallel", 64, 1, 900.0),
        scenario("supervisor", "parallel", 64, 2, 1_700.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    for family in [Family::ThroughputVsConsumers, Family::ParallelVsSequenced] {
        let svg =
            chartgen::render_to_string(&doc, family, Mode::Light).expect("chart should render");
        let caption = texts(&svg)
            .into_iter()
            .map(|(_, _, _, t)| t)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            caption.contains("sqs") && caption.contains("shape only"),
            "{family:?}: the sqs supervisor rows must be charted shape-only: {caption}"
        );
        assert!(
            caption.contains("measured as supervisor"),
            "{family:?}: the alias must be captioned: {caption}"
        );
        assert!(
            !caption.contains("sqs / parallel: spelled supervisor")
                && !caption.contains("sqs: spelled supervisor"),
            "{family:?}: a measured alias was captioned as absent: {caption}"
        );
        // The document's own declaration for the canonical flow is the
        // explanation of the alias; it is carried verbatim, not dropped.
        assert!(
            caption.contains("measured as supervisor — spelled supervisor on this backend"),
            "{family:?}: the declared reason for the alias was dropped: {caption}"
        );
    }
}

#[test]
fn a_payload_size_outside_the_harness_set_is_refused() {
    // The formatter truncates: 1536 B would be labelled "1 KiB" beside the
    // real 1 KiB category. The harness only ever writes 64, 1024 and 65536.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 1536, 1, 50_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("payload"), "wrong reason: {what}")
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
    let failed = r#"{
          "backend": "nats", "representative": true,
          "results": [],
          "failures": [{ "flow": "consume_parallel", "payload_bytes": 4096, "consumers": 1, "error": "x" }],
          "unsupported": []
        }"#;
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), failed)));
    assert!(
        matches!(
            chartgen::validate(&doc),
            Err(ChartError::MalformedRow { .. })
        ),
        "a failure coordinate outside the payload set must be refused too"
    );
}

#[test]
fn an_absurdly_long_backend_name_does_not_panic_the_layout() {
    // Legend and caption geometry are computed from document strings; a
    // pathological name must end in a refusal or a rendered chart, never a
    // panic from overflowing arithmetic.
    let name = "b".repeat(300_000);
    let run = format!(
        r#"{{
          "backend": "{name}", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 50_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    for family in Family::ALL {
        // Either outcome is acceptable; a panic is not.
        let _ = chartgen::render_to_string(&doc, family, Mode::Light);
    }
}

// ── Review round 5 ──────────────────────────────────────────────────────────

#[test]
fn an_unknown_handler_label_is_refused() {
    // The marker is derived from a closed set of handler profiles; a label
    // the harness never writes cannot certify any marker, least of all
    // `framework`.
    let row = scenario("consume_parallel", "parallel", 64, 1, 50_000.0).replace(
        "\"handler\": \"zero (no-op)\"",
        "\"handler\": \"glacial (10s)\"",
    );
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{row}], "failures": [], "unsupported": []
        }}"#
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::validate(&doc) {
        Err(ChartError::MalformedRow { what, .. }) => {
            assert!(what.contains("glacial"), "wrong reason: {what}")
        }
        other => panic!("expected MalformedRow, got {other:?}"),
    }
}

#[test]
fn a_shape_only_lower_bound_bar_still_says_lower_bound() {
    // A non-representative fifo bar contributes shape from a setup-bound
    // window; "shape only" does not tell the reader the shape's fifo bar is
    // itself only a lower bound.
    let run = format!(
        r#"{{
          "backend": "sqs", "representative": false,
          "broker": {{ "name": "LocalStack", "version": "x", "deployment": "localstack" }},
          "results": [{},{}], "failures": [],
          "unsupported": [{{ "flow": "consume_batch", "reason": "no batch on sqs" }}]
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 900.0),
        scenario("consume_fifo", "fifo", 64, 1, 400.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        caption.contains("sqs / sequenced (fifo): lower bound")
            || caption.contains("sqs / sequenced (fifo): lower bound (muted bar)"),
        "the shape-only fifo bar lost its lower-bound disclosure: {caption}"
    );
}

// ── Review round 6 ──────────────────────────────────────────────────────────

#[test]
fn a_failure_only_line_slice_is_captioned_as_failed_not_as_a_gap() {
    // kafka's only presence in the slice is a recorded failure; the caption
    // must say failed, and must not also call the backend a gap.
    let run = r#"{
          "backend": "kafka", "representative": true,
          "results": [],
          "failures": [{ "flow": "consume_parallel", "payload_bytes": 64, "consumers": 1,
                         "handler": "zero (no-op)", "error": "timeout after 60s" }],
          "unsupported": []
        }"#;
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(caption.contains("kafka: failed to run"), "{caption}");
    assert!(
        !caption.contains("kafka: no measurement"),
        "a failed cell was captioned as a gap: {caption}"
    );
    assert!(
        texts(&svg).iter().any(|(_, _, _, t)| t == "kafka (failed)"),
        "the legend must mark the failed backend"
    );
}

#[test]
fn disjoint_worker_counts_are_not_captioned_as_shared() {
    // parallel measured only at 1 worker, fifo only at 8: there is no shared
    // count, and the caption must say so instead of claiming one.
    let run = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{},{}], "failures": [],
          "unsupported": [{{ "flow": "consume_batch", "reason": "n/a" }}]
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 60_000.0),
        scenario("consume_fifo", "fifo", 64, 8, 20_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        caption
            .contains("kafka / sequenced (fifo): measured at 8 workers (no worker count is shared"),
        "{caption}"
    );
    assert!(
        !caption.contains("kafka / sequenced (fifo): measured at 8 workers (the least-parallel count the measured modes share)"),
        "a disjoint count was captioned as shared: {caption}"
    );
}

// ── Review round 7 ──────────────────────────────────────────────────────────

#[test]
fn a_handler_profile_whose_cells_all_failed_still_reaches_the_provenance() {
    // The provenance line states which handler profiles the dataset was
    // measured under; a profile whose every cell failed was still run.
    let run = r#"{
          "backend": "kafka", "representative": true,
          "results": [],
          "failures": [{ "flow": "consume_parallel", "payload_bytes": 64, "consumers": 1,
                         "handler": "heavy (1-5s)", "error": "timeout after 600s" }],
          "unsupported": []
        }"#;
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    for (family, svg) in render_all(&doc) {
        let caption = texts(&svg)
            .into_iter()
            .map(|(_, _, _, t)| t)
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            caption.contains("heavy (1-5s)"),
            "{family:?}: the failed cells' handler profile is missing from the provenance: {caption}"
        );
    }
}

#[test]
fn an_unsupported_entry_for_a_flow_unknown_to_the_chart_is_named() {
    // Unknown flows are tolerated as additive, but a declared entry for one
    // must not vanish just because no column exists for it.
    let run = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "broker": {{ "name": "in-process", "version": "n/a", "deployment": "in-process" }},
          "results": [{}],
          "failures": [],
          "unsupported": [{{ "flow": "consume_streaming", "reason": "no streaming primitive yet" }}]
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 50_000.0)
    );
    let doc = parse(&document(&run));
    let svg = chartgen::render_to_string(&doc, Family::FrameworkOverhead, Mode::Light)
        .expect("chart should render");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        caption.contains("consume_streaming") && caption.contains("no streaming primitive yet"),
        "the unknown-flow declaration vanished: {caption}"
    );
}

// ── Review round 8 ──────────────────────────────────────────────────────────

#[test]
fn contradictory_or_duplicate_declarations_are_refused() {
    let failure = r#"{ "flow": "consume_parallel", "payload_bytes": 64, "consumers": 1,
                       "handler": "zero (no-op)", "error": "timeout" }"#;
    let cases = [
        (
            "failed and declared unsupported",
            format!(
                r#"{{ "backend": "kafka", "representative": true, "results": [],
                     "failures": [{failure}],
                     "unsupported": [{{ "flow": "consume_parallel", "reason": "not here" }}] }}"#
            ),
        ),
        (
            "duplicate unsupported declarations",
            format!(
                r#"{{ "backend": "kafka", "representative": true, "results": [{}],
                     "failures": [],
                     "unsupported": [{{ "flow": "consume_batch", "reason": "a" }},
                                     {{ "flow": "consume_batch", "reason": "b" }}] }}"#,
                scenario("publish_single", "parallel", 64, 1, 10.0)
            ),
        ),
        (
            "failure without a handler",
            r#"{ "backend": "kafka", "representative": true, "results": [],
                 "failures": [{ "flow": "consume_parallel", "payload_bytes": 64, "consumers": 1,
                                "error": "timeout" }],
                 "unsupported": [] }"#
                .to_string(),
        ),
    ];
    for (what, run) in cases {
        let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
        assert!(
            matches!(
                chartgen::validate(&doc),
                Err(ChartError::MalformedRow { .. })
            ),
            "{what}: expected MalformedRow, got {:?}",
            chartgen::validate(&doc)
        );
        for family in Family::ALL {
            assert!(
                chartgen::render_to_string(&doc, family, Mode::Light).is_err(),
                "{what}: {family:?} rendered a refused document"
            );
        }
    }
}

// ── Review round 9 ──────────────────────────────────────────────────────────

#[test]
fn a_setup_bound_row_the_harness_would_have_called_framework_is_refused() {
    // A negligible handler on a barrier flow with a recorded setup window
    // and a window over the floor is exactly what the harness stamps
    // `framework`; `setup_bound` on such a row is a hand-edit. Likewise a
    // barrier-less flow never records a setup window at all.
    let over_floor = scenario_with_window(
        "consume_parallel",
        "parallel",
        64,
        1,
        50_000.0,
        "setup_bound",
        2.0,
    );
    let fifo_with_setup = scenario("consume_fifo", "fifo", 64, 1, 4_000.0)
        .replace("\"setup_secs\": null", "\"setup_secs\": 0.4");
    for (what, row) in [
        ("setup_bound over the floor", over_floor),
        ("fifo with setup_secs", fifo_with_setup),
    ] {
        let run = format!(
            r#"{{
              "backend": "kafka", "representative": true,
              "results": [{row}], "failures": [], "unsupported": []
            }}"#
        );
        let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
        assert!(
            matches!(
                chartgen::validate(&doc),
                Err(ChartError::MalformedRow { .. })
            ),
            "{what}: expected MalformedRow, got {:?}",
            chartgen::validate(&doc)
        );
    }
}

#[test]
fn a_legend_entry_that_cannot_fit_the_canvas_is_refused() {
    let name = "b".repeat(200);
    let run = format!(
        r#"{{
          "backend": "{name}", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 50_000.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    match chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light) {
        Err(ChartError::Render(msg)) => assert!(msg.contains("legend"), "{msg}"),
        other => panic!("expected a Render refusal, got {other:?}"),
    }
}

// ── Review round 10 ─────────────────────────────────────────────────────────

#[test]
fn every_published_bar_carries_a_value_label() {
    // On a shared linear axis one fast backend makes the others a few
    // pixels tall; the label is what keeps them readable. Lower bounds are
    // labelled with the ≥ they are.
    let doc = parse(&document(&inmemory_run(true)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    let labels: Vec<String> = texts(&svg).into_iter().map(|(_, _, _, t)| t).collect();
    assert!(
        labels.iter().any(|t| t == "10k"),
        "parallel bar has no value label: {labels:?}"
    );
    assert!(
        labels.iter().any(|t| t == "≥4.0k"),
        "the lower-bound bar must be labelled with ≥: {labels:?}"
    );
    let svg = chartgen::render_to_string(&doc, Family::FrameworkOverhead, Mode::Light)
        .expect("chart should render");
    let labels: Vec<String> = texts(&svg).into_iter().map(|(_, _, _, t)| t).collect();
    // 1e9 / 10_000 msg/s = 100,000 ns
    assert!(
        labels.iter().any(|t| t == "100k"),
        "overhead bar has no value label: {labels:?}"
    );
}

#[test]
fn a_shape_only_bar_carries_no_value_and_no_absolute_bound() {
    let run = format!(
        r#"{{
          "backend": "sqs", "representative": false,
          "broker": {{ "name": "LocalStack", "version": "x", "deployment": "localstack" }},
          "results": [{},{}], "failures": [],
          "unsupported": [{{ "flow": "consume_batch", "reason": "no batch on sqs" }}]
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 777.0),
        scenario("consume_fifo", "fifo", 64, 1, 333.0)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));
    let svg = chartgen::render_to_string(&doc, Family::ParallelVsSequenced, Mode::Light)
        .expect("chart should render");
    let labels: Vec<String> = texts(&svg).into_iter().map(|(_, _, _, t)| t).collect();
    assert!(
        !labels.iter().any(|t| t == "777" || t == "≥333"),
        "a shape-only bar was labelled with its magnitude: {labels:?}"
    );
    let caption = labels.join(" ");
    assert!(
        caption.contains("sqs / sequenced (fifo): lower bound from a window")
            && caption.contains("its height is not the bound"),
        "{caption}"
    );
    assert!(
        !caption.contains("sqs / sequenced (fifo): lower bound (muted bar)"),
        "a shape-only bar was captioned with an absolute bound: {caption}"
    );
}

// ── Review round 11 ─────────────────────────────────────────────────────────

#[test]
fn a_document_that_ran_no_cell_states_so_in_the_provenance() {
    let doc = parse(&document(&declared_only_run(
        "sqs",
        "not measured in this document",
    )));
    // Only the latency panel renders without an in-process run or any data.
    let svg = chartgen::render_to_string(&doc, Family::DispatchLatency, Mode::Light)
        .expect("panel should render");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        caption.contains("handler: none — no cells were run"),
        "an empty document must state its missing handler profile: {caption}"
    );
}

// ── Review round 13 ─────────────────────────────────────────────────────────

#[test]
fn a_slice_with_no_publishable_point_renders_its_account_instead_of_refusing() {
    // A faster host can leave every 64 B cell under the window floor. That
    // is a chart full of explanations, not a reason to refuse every family.
    let rows: Vec<String> = [1u32, 2, 4]
        .iter()
        .map(|c| {
            scenario_with_window(
                "consume_parallel",
                "parallel",
                64,
                *c,
                500_000.0,
                "setup_bound",
                0.3,
            )
        })
        .collect();
    let run = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "broker": {{ "name": "in-process", "version": "n/a", "deployment": "in-process" }},
          "results": [{},{}], "failures": [], "unsupported": []
        }}"#,
        rows.join(","),
        scenario("consume_parallel", "parallel", 1024, 1, 40_000.0)
    );
    let doc = parse(&document(&run));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("a wholly withheld slice must still render its account");
    let caption = texts(&svg)
        .into_iter()
        .map(|(_, _, _, t)| t)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        caption.contains("inmemory: measured, but no drain rate")
            && caption.contains("window under 1 s")
            && caption.contains("consumers 1, 2, 4"),
        "{caption}"
    );
    assert!(!svg.contains("500000") && !svg.contains("500k"));
}

// ── The redesign's own guards: slots, end labels, log decades ───────────────

/// A run for an arbitrary backend name with one plottable framework row, so
/// slot-assignment tests can put unknown backends on a line chart.
fn plottable_run(backend: &str) -> String {
    format!(
        r#"{{
          "backend": "{backend}", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario_with_cost("consume_parallel", "parallel", 64, 1, 12_000.0, "framework")
    )
}

#[test]
fn two_unknown_backends_take_the_overflow_slots_and_a_third_is_refused() {
    // The palette has six fixed backend slots and two validated spares. Two
    // unknown backends render (on the spare hues — never a generated one);
    // a third has no validated hue left and must be a loud error, not an
    // invented colour.
    let two = parse(&document(&format!(
        "{},{}",
        plottable_run("zeromq"),
        plottable_run("pulsar")
    )));
    let svg = chartgen::render_to_string(&two, Family::ThroughputVsConsumers, Mode::Light)
        .expect("two unknown backends fit the overflow slots");
    // Which spare each takes is BTreeMap order (pulsar then zeromq); the
    // whitelist test proves the hues come from the validated set. Here it is
    // enough that both render.
    assert!(svg.contains("zeromq") && svg.contains("pulsar"));

    let three = parse(&document(&format!(
        "{},{},{}",
        plottable_run("zeromq"),
        plottable_run("pulsar"),
        plottable_run("mqtt")
    )));
    match chartgen::render_to_string(&three, Family::ThroughputVsConsumers, Mode::Light) {
        Err(ChartError::Render(msg)) => assert!(
            msg.contains("palette slot"),
            "the refusal must name the missing slot: {msg}"
        ),
        other => panic!("a third unknown backend must refuse to render: {other:?}"),
    }
}

#[test]
fn every_plotted_line_carries_a_direct_end_label() {
    // The end labels are the identity relief for the sub-3:1 light-mode
    // series (and the CVD channel): each plotted backend's name appears once
    // in the legend and once at its line's end. The run needs several points
    // in the headline slice so the legend entry is the bare backend name — a
    // single-point run is labelled "inmemory (single point)" and would count
    // the end label alone.
    let rows: Vec<String> = [1u32, 2, 4]
        .iter()
        .map(|c| {
            scenario(
                "consume_parallel",
                "parallel",
                64,
                *c,
                10_000.0 * f64::from(*c),
            )
        })
        .collect();
    let run = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows.join(",")
    );
    let doc = parse(&document(&run));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    let names: Vec<String> = texts(&svg)
        .into_iter()
        .map(|(_, _, _, c)| c)
        .filter(|c| c == "inmemory")
        .collect();
    assert_eq!(
        names.len(),
        2,
        "expected the legend entry plus exactly one end label"
    );
}

#[test]
fn an_absolute_line_axis_is_log_scaled_with_decade_ticks() {
    // A linear axis lets one fast backend crush every other line into the
    // baseline; the absolute line families publish on a log axis instead,
    // and the axis says so on its face.
    let doc = parse(&document(&inmemory_run(true)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    assert!(
        svg.contains("log scale"),
        "the y-axis description must name the log scale"
    );
    // The fixture's family-1 rows sit in the tens of thousands; a linear
    // axis would label 20k/30k-style ticks, a decade axis labels the decade.
    let ticks: Vec<String> = texts(&svg)
        .into_iter()
        .filter(|(x, _, _, c)| *x < Y_TICK_BAND_X && !c.is_empty())
        .map(|(_, _, _, c)| c)
        .collect();
    assert!(
        ticks.iter().any(|t| t == "10k" || t == "10.0k"),
        "expected a decade tick on the log axis, got {ticks:?}"
    );
}

// ── Redesign diff-review round-1 guards ─────────────────────────────────────

#[test]
fn a_long_backend_name_widens_the_end_label_gutter() {
    // The overflow palette slots admit unknown backends with names far wider
    // than the fixed six. The gutter must grow to fit what it labels — a
    // clipped end label mis-names the series it exists to identify.
    let rows: Vec<String> = [1u32, 2, 4]
        .iter()
        .map(|c| {
            scenario(
                "consume_parallel",
                "parallel",
                64,
                *c,
                9_000.0 * f64::from(*c),
            )
        })
        .collect();
    let run = format!(
        r#"{{
          "backend": "charlie-backend-name", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows.join(",")
    );
    let doc = parse(&document(&run));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("a 20-char backend name must render");
    let names: Vec<(f64, f64, f64, String)> = texts(&svg)
        .into_iter()
        .filter(|(_, _, _, c)| c == "charlie-backend-name")
        .collect();
    assert_eq!(
        names.len(),
        2,
        "expected the legend entry plus the end label"
    );
    for (x, _, size, content) in names {
        let right = x + est_width(size, &content);
        assert!(
            right <= f64::from(chartgen::WIDTH),
            "the label runs off the canvas (x={x}, est. right edge {right:.0})"
        );
    }
}

#[test]
fn an_end_label_wider_than_the_gutter_cap_is_refused() {
    // Same contract as the legend: document-supplied text either fits or the
    // chart refuses loudly. A name too wide for the capped gutter must never
    // be silently clipped by the viewBox.
    let name = "a-backend-name-so-long-it-cannot-be-labelled";
    let rows: Vec<String> = [1u32, 2, 4]
        .iter()
        .map(|c| {
            scenario(
                "consume_parallel",
                "parallel",
                64,
                *c,
                9_000.0 * f64::from(*c),
            )
        })
        .collect();
    let run = format!(
        r#"{{
          "backend": "{name}", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows.join(",")
    );
    let doc = parse(&document(&run));
    match chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light) {
        Err(ChartError::Render(msg)) => assert!(
            msg.contains("end-label"),
            "the refusal must name the end-label gutter: {msg}"
        ),
        other => panic!("an unlabellable name must refuse to render: {other:?}"),
    }
}

#[test]
fn a_refusal_panel_that_cannot_fit_its_band_is_refused() {
    // The dispatch-latency accounting lines used to travel through frame()'s
    // footer, where the caption-block guard counted them; drawn as a free
    // panel block they need the same loud refusal when they outgrow the band
    // the frame guard proves free — never a silent overprint of the footer.
    // 20 long-named backends wrap to ~28 accounting lines (~470px), taller
    // than the whole footer-to-chrome gap this fixture leaves free.
    let runs: Vec<String> = (0..20)
        .map(|i| {
            format!(
                r#"{{
                  "backend": "{}-{i}", "representative": true,
                  "results": [{}], "failures": [], "unsupported": []
                }}"#,
                "x".repeat(148),
                scenario("consume_parallel", "parallel", 64, 1, 5_000.0)
            )
        })
        .collect();
    let doc = parse(&document(&runs.join(",")));
    match chartgen::render_to_string(&doc, Family::DispatchLatency, Mode::Light) {
        Err(ChartError::Render(msg)) => assert!(
            msg.contains("panel"),
            "the refusal must name the panel band: {msg}"
        ),
        other => panic!("an oversized panel block must refuse to render: {other:?}"),
    }
}

#[test]
fn a_log_axis_spanning_more_decades_than_f64_is_refused() {
    // validate() accepts any finite positive throughput — including
    // subnormals like 1e-310. An axis floored there makes y_hi/y_lo overflow
    // to +inf, and the shape-only geometric mapping would write saturated
    // garbage coordinates into a clean-looking SVG. Loud refusal instead.
    let tiny = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 1e-310)
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), tiny)));
    match chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light) {
        Err(ChartError::Render(msg)) => assert!(
            msg.contains("decades"),
            "the refusal must name the unrepresentable span: {msg}"
        ),
        other => panic!("an unrepresentable axis span must refuse to render: {other:?}"),
    }
}

#[test]
fn sub_unit_decade_ticks_carry_distinct_labels() {
    // A log floor below 1 is legal (any positive throughput validates), and
    // every sub-0.05 decade used to render as the same "0.0" tick — several
    // distinct decades wearing one wrong label. The floor decade itself is
    // not emitted by the tick generator, so the fixture reaches 1e-8 to put
    // seven sub-unit decades in the interior — deep enough that the decades
    // past six decimals must switch to exponent form ("1e-7") rather than
    // outgrow the ~90px y-label area and clip at the viewBox.
    let rows = [
        scenario("consume_parallel", "parallel", 64, 1, 0.00000002),
        scenario("consume_parallel", "parallel", 64, 2, 0.5),
        scenario("consume_parallel", "parallel", 64, 4, 0.9),
    ];
    let run = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows.join(",")
    );
    let doc = parse(&document(&run));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("sub-unit throughput must render");
    let ticks: Vec<String> = texts(&svg)
        .into_iter()
        .filter(|(x, _, _, c)| *x < Y_TICK_BAND_X && tick_shaped(c))
        .map(|(_, _, _, c)| c)
        .collect();
    assert!(
        ticks.iter().any(|t| t == "0.01"),
        "the 0.01 decade must be labelled as itself, got {ticks:?}"
    );
    assert!(
        ticks.iter().any(|t| t == "1e-7"),
        "a decade past six decimals must wear exponent form, got {ticks:?}"
    );
    // Every tick must FIT left of the axis, not merely exist: ticks are
    // end-anchored, so the estimated extent runs leftward from x, and a
    // label wider than the y-label area clips at the viewBox. This is the
    // loud coupling between fmt_count's exponent cutover and the axis
    // geometry — shrink the y-label area and this fails instead of
    // shipping clipped magnitudes.
    for (x, _, size, c) in texts(&svg)
        .into_iter()
        .filter(|(x, _, _, c)| *x < Y_TICK_BAND_X && tick_shaped(c))
    {
        assert!(
            x - est_width(size, &c) >= 0.0,
            "tick {c:?} extends past the left canvas edge (anchor x={x})"
        );
    }
    let mut deduped = ticks.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        ticks.len(),
        "no two ticks may wear the same label: {ticks:?}"
    );
}

#[test]
fn a_mid_chart_end_label_is_not_displaced_by_a_far_away_gutter_label() {
    // Label deconfliction is geometric: a gutter label and a mid-chart
    // label whose x-extents sit ~700px apart never actually overlap, and
    // displacing the mid-chart one detaches it from its leaderless dot —
    // the reader then hangs the name on whichever other line passes
    // through that y.
    //
    // Byte-determinism makes the probe exact: the label-to-dot anchor offset
    // in a control render (kafka alone near the top, nothing within spacing
    // range) must equal the offset when another series' gutter label sits
    // within the 14px spacing window at a far x.
    let kafka_rows = [
        scenario("consume_parallel", "parallel", 64, 1, 10_000.0),
        scenario("consume_parallel", "parallel", 64, 2, 9_000.0),
    ];
    let kafka = format!(
        r#"{{
          "backend": "kafka", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        kafka_rows.join(",")
    );
    let inmemory = |final_throughput: f64| {
        let rows = [
            scenario("consume_parallel", "parallel", 64, 1, 100.0),
            scenario("consume_parallel", "parallel", 64, 2, 90.0),
            scenario("consume_parallel", "parallel", 64, 4, final_throughput),
        ];
        format!(
            r#"{{
              "backend": "inmemory", "representative": true,
              "results": [{}], "failures": [], "unsupported": []
            }}"#,
            rows.join(",")
        )
    };
    let offset_of = |inmemory_final: f64| -> f64 {
        let doc = parse(&document(&format!(
            "{},{}",
            inmemory(inmemory_final),
            kafka
        )));
        let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
            .expect("chart should render");
        // kafka's endpoint dot: the rightmost r=4 circle in kafka's hue —
        // derived from the palette (fixed slot 1) through the one hex
        // encoding chartgen owns, never a literal that a retune would
        // orphan or a re-spelled encoding that could drift in case.
        let kafka_hex = format!("#{}", chartgen::hex(&Mode::Light.series()[1]));
        let (cx, cy) = circles(&svg)
            .into_iter()
            .filter(|(_, _, r, fill)| *r == 4.0 && *fill == kafka_hex)
            .map(|(cx, cy, _, _)| (cx, cy))
            .fold((f64::MIN, f64::MIN), |acc, (cx, cy)| {
                if cx > acc.0 { (cx, cy) } else { acc }
            });
        assert!(cx > f64::MIN, "kafka's endpoint dot not found");
        // kafka's mid-chart end label: the "kafka" text inside the plot
        // band. The legend copy is excluded by geometry — legend rows sit at
        // y ≈ 74–90, end labels at ≥ plot_top + spacing ≈ 103 — never by
        // emission order, so a draw-order refactor cannot silently point
        // this probe at the legend.
        let label_y = texts(&svg)
            .into_iter()
            .filter(|(x, y, _, c)| c == "kafka" && *x > 350.0 && *y > 100.0)
            .map(|(_, y, _, _)| y)
            .next()
            .expect("kafka's end label not found");
        label_y - cy
    };
    // Control: inmemory's gutter label sits hundreds of px below kafka's.
    let control = offset_of(80.0);
    // Probe: inmemory's final point lands within the 14px spacing window of
    // kafka's endpoint — at the far right, in the gutter column.
    let probed = offset_of(9_100.0);
    assert!(
        (control - probed).abs() < f64::EPSILON,
        "the far-away gutter label displaced kafka's mid-chart label \
         (control offset {control}, probed offset {probed})"
    );
}

#[test]
fn every_ink_role_and_series_hue_clears_its_surface() {
    // The palette whitelist proves a render only uses the mode's constants —
    // it cannot catch a typo'd constant itself. This holds the WCAG floor
    // for every ink against the surface it prints on, so a near-surface
    // secondary (invisible captions) fails loudly instead of shipping.
    fn luminance(c: &RGBColor) -> f64 {
        let lin = |v: u8| {
            let v = f64::from(v) / 255.0;
            if v <= 0.04045 {
                v / 12.92
            } else {
                ((v + 0.055) / 1.055).powf(2.4)
            }
        };
        0.2126 * lin(c.0) + 0.7152 * lin(c.1) + 0.0722 * lin(c.2)
    }
    fn contrast(a: &RGBColor, b: &RGBColor) -> f64 {
        let (la, lb) = (luminance(a), luminance(b));
        (la.max(lb) + 0.05) / (la.min(lb) + 0.05)
    }
    for mode in Mode::ALL {
        let (surface, [primary, secondary, muted]) = mode.inks();
        for (role, ink, floor) in [
            ("primary", primary, 7.0),
            ("secondary", secondary, 4.5),
            ("muted", muted, 3.0),
        ] {
            let ratio = contrast(&ink, &surface);
            assert!(
                ratio >= floor,
                "{mode:?} {role} ink contrast {ratio:.2} is under its {floor}:1 floor"
            );
        }
        for (i, hue) in mode.series().iter().enumerate() {
            let ratio = contrast(hue, &surface);
            assert!(
                ratio >= 1.9,
                "{mode:?} series slot {i} contrast {ratio:.2} is under the 1.9:1 mark floor"
            );
        }
        // The pre-blended muted fills are, by construction, the lowest-
        // contrast marks in the system (shape-only and lower-bound bars).
        // Muted is the point — the floor is set to catch invisibility (a
        // typo'd blend lands near 1.0), not to promise full a11y contrast.
        for (i, fill) in mode.muted_fills().iter().enumerate() {
            let ratio = contrast(fill, &surface);
            assert!(
                ratio >= 1.4,
                "{mode:?} muted fill for slot {i} contrast {ratio:.2} is under the 1.4:1 \
                 visibility floor"
            );
        }
    }
}

// ── Redesign diff-review round-2 guards ─────────────────────────────────────

#[test]
fn labels_in_adjacent_columns_are_still_deconflicted() {
    // Round 1's per-column allocator assumed only same-column labels can
    // collide; at 12 categories the column pitch (~66px) is narrower than
    // an 8-char label (72px), so labels anchored in adjacent columns at the
    // same y overprint. Collision is geometry, not column identity: any two
    // labels whose estimated x-extents overlap must keep the 14px vertical
    // spacing.
    let counts: Vec<u32> = (0..12).map(|i| 1u32 << i).collect();
    let series = |backend: &str, upto: usize, v: f64| -> String {
        let rows: Vec<String> = counts[..upto]
            .iter()
            .map(|c| scenario("consume_parallel", "parallel", 64, *c, v))
            .collect();
        format!(
            r#"{{
              "backend": "{backend}", "representative": true,
              "results": [{}], "failures": [], "unsupported": []
            }}"#,
            rows.join(",")
        )
    };
    // rabbitmq ends at category 9, redis at category 10, same throughput —
    // same desired y, overlapping x-extents, different columns.
    let doc = parse(&document(&format!(
        "{},{},{}",
        series("inmemory", 12, 50_000.0),
        series("rabbitmq", 10, 500.0),
        series("redis", 11, 500.0)
    )));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("chart should render");
    // One parse; the legend copies are excluded by geometry (legend rows at
    // y ≈ 74–90, end labels inside the plot band ≥ ~103), never by emission
    // order — a draw-order refactor cannot silently point this at a legend
    // row and read row spacing as deconfliction.
    let all_texts = texts(&svg);
    let label_y = |name: &str| -> f64 {
        all_texts
            .iter()
            .filter(|(x, y, _, c)| c == name && *x > 300.0 && *y > 100.0)
            .map(|(_, y, _, _)| *y)
            .next()
            .unwrap_or_else(|| panic!("{name}'s end label not found"))
    };
    let dy = (label_y("rabbitmq") - label_y("redis")).abs();
    assert!(
        dy >= 13.0,
        "overlapping-extent labels in adjacent columns must be separated \
         vertically (got {dy:.0}px)"
    );
}

#[test]
fn a_long_name_that_never_enters_the_gutter_does_not_refuse_the_chart() {
    // The gutter is sized (and its cap enforced) only for names it will
    // actually hold. A 26-char backend whose line stops at the first
    // category labels at its endpoint, not in the gutter — refusing the
    // whole chart for it was round 1 overshooting.
    let long_name = "a-backend-name-of-26-chars";
    let short = format!(
        r#"{{
          "backend": "{long_name}", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        scenario("consume_parallel", "parallel", 64, 1, 5_000.0)
    );
    // inmemory spans consumers 1/2/4 in the headline slice, so the sweep has
    // three categories and the long name's single point sits at the first —
    // never the final, never the gutter.
    let sweep: Vec<String> = [1u32, 2, 4]
        .iter()
        .map(|c| {
            scenario(
                "consume_parallel",
                "parallel",
                64,
                *c,
                10_000.0 * f64::from(*c),
            )
        })
        .collect();
    let inmemory = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        sweep.join(",")
    );
    let doc = parse(&document(&format!("{inmemory},{short}")));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("a long mid-chart name must not refuse the chart");
    for (x, _, size, content) in texts(&svg)
        .into_iter()
        .filter(|(_, _, _, c)| c == long_name)
    {
        let right = x + est_width(size, &content);
        assert!(
            right <= f64::from(chartgen::WIDTH),
            "the mid-chart label must fit the canvas (x={x}, est. right {right:.0})"
        );
    }
}

#[test]
fn a_mid_chart_label_that_cannot_fit_rightward_flips_left() {
    // A long name whose line ends deep into a dense sweep cannot extend
    // rightward without crossing the canvas edge; it flips to the left of
    // its endpoint (end-anchored) instead of clipping or refusing.
    let long_name = "a-backend-name-of-26-chars";
    let counts: Vec<u32> = (0..12).map(|i| 1u32 << i).collect();
    let rows = |upto: usize| -> String {
        counts[..upto]
            .iter()
            .map(|c| scenario("consume_parallel", "parallel", 64, *c, 9_000.0))
            .collect::<Vec<_>>()
            .join(",")
    };
    let ending_mid = format!(
        r#"{{
          "backend": "{long_name}", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows(11)
    );
    let reaching_end = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows(12)
    );
    let doc = parse(&document(&format!("{},{}", reaching_end, ending_mid)));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light)
        .expect("a fit-by-flip label must not refuse the chart");
    // Read the label's own element so the anchor is visible: a flipped
    // label is end-anchored and its extent runs leftward from x. The
    // legend copy is excluded by geometry (legend rows at y ≈ 74–90, end
    // labels inside the plot band), never by emission order.
    let chunk = svg
        .split("<text ")
        .skip(1)
        .find(|c| {
            c.contains(long_name)
                && svg_attr(c, "x")
                    .and_then(|v| v.parse::<f64>().ok())
                    .is_some_and(|x| x > 200.0)
                && svg_attr(c, "y")
                    .and_then(|v| v.parse::<f64>().ok())
                    .is_some_and(|y| y > 100.0)
        })
        .expect("the end label must exist");
    let x: f64 = svg_attr(chunk, "x")
        .and_then(|v| v.parse().ok())
        .expect("label x");
    let size: f64 = svg_attr(chunk, "font-size")
        .and_then(|v| v.parse().ok())
        .expect("label size");
    assert_eq!(
        svg_attr(chunk, "text-anchor").as_deref(),
        Some("end"),
        "a label with no rightward room must flip to end-anchored"
    );
    let left = x - est_width(size, long_name);
    assert!(
        left >= 0.0 && x <= f64::from(chartgen::WIDTH),
        "the flipped label must sit fully on canvas (est. left {left:.0}, x={x})"
    );
}

// ── Redesign diff-review round-3 guards ─────────────────────────────────────

#[test]
fn a_label_fitting_neither_side_of_its_endpoint_is_refused() {
    // A flipped label must clear plot-left — across the y-label band it
    // overprints the axis ticks. A name too wide for either side of its
    // endpoint refuses loudly rather than rendering an unreadable axis.
    // 70 chars ≈ 630 estimated px: too wide to extend right from category 8
    // of 12 (~300px of room to the frame margin) and too wide to clear
    // plot-left going leftward (~540px of room) — yet inside the legend's
    // own budget, so the label refusal is the one that fires.
    let long_name = format!("{}long-xx", "really-".repeat(9));
    let counts: Vec<u32> = (0..12).map(|i| 1u32 << i).collect();
    let rows = |upto: usize| -> String {
        counts[..upto]
            .iter()
            .map(|c| scenario("consume_parallel", "parallel", 64, *c, 9_000.0))
            .collect::<Vec<_>>()
            .join(",")
    };
    let ending_mid = format!(
        r#"{{
          "backend": "{long_name}", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows(9)
    );
    let reaching_end = format!(
        r#"{{
          "backend": "inmemory", "representative": true,
          "results": [{}], "failures": [], "unsupported": []
        }}"#,
        rows(12)
    );
    let doc = parse(&document(&format!("{},{}", reaching_end, ending_mid)));
    match chartgen::render_to_string(&doc, Family::ThroughputVsConsumers, Mode::Light) {
        Err(ChartError::Render(msg)) => assert!(
            msg.contains("neither side"),
            "the refusal must name the unplaceable label: {msg}"
        ),
        other => panic!("an unplaceable end label must refuse to render: {other:?}"),
    }
}
