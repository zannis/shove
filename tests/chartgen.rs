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

use chartgen::{ChartError, Document, Family};

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
    // Since v3 every `consume_batch` row carries its batch knobs, and no
    // other flow's row may carry them.
    let batch_knobs = if flow == "consume_batch" {
        r#""max_batch_size": 500, "max_batch_age_ms": 200,"#
    } else {
        ""
    };
    format!(
        r#"{{
          "flow": "{flow}", "mode": "{mode}", "payload_bytes": {payload},
          "tier": "moderate", "messages": 5000, "consumers": {consumers},
          "handler": "zero (no-op)",
          "handler_cost": "{handler_cost}",
          "setup_secs": 0.4,
          {batch_knobs}
          "throughput_msg_per_sec": {throughput},
          "dispatch_p50_ms": 1.5, "dispatch_p95_ms": 4.0, "dispatch_p99_ms": 9.0,
          "e2e_p50_ms": 1.5, "e2e_p95_ms": 4.0, "e2e_p99_ms": 9.0,
          "scaling_efficiency": 1.0, "peak_rss_mb": 1.0, "cpu_pct": 100.0,
          "duration_secs": 2.0
        }}"#
    )
}

/// A v4 row whose marker is what the harness would derive for a zero handler:
/// publish flows carry `no_handler`, the barrier-less flows (`consume_fifo`,
/// `dlq_drain`, `autoscaler`) carry `setup_bound`, everything else is a
/// `framework` row with a separated window.
fn scenario(flow: &str, mode: &str, payload: u64, consumers: u32, throughput: f64) -> String {
    let cost = match flow {
        "publish_single" | "publish_batch" => "no_handler",
        "consume_fifo" | "dlq_drain" | "autoscaler" => "setup_bound",
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
            let svg = chartgen::render_to_string(doc, f)
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
                chartgen::render_to_string(&doc, family),
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
                chartgen::render_to_string(&doc, family),
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
        scenario_with_cost(
            "consume_parallel",
            "parallel",
            1024,
            1,
            424_242.0,
            "setup_bound"
        ),
        scenario_with_cost(
            "consume_parallel",
            "parallel",
            64,
            1,
            434_343.0,
            "setup_bound"
        )
    );
    let doc = parse(&document(&format!("{},{}", inmemory_run(true), run)));

    for family in [Family::ThroughputVsConsumers, Family::ThroughputVsPayload] {
        let svg = chartgen::render_to_string(&doc, family).expect("chart should render");
        for magnitude in ["424242", "424.2k", "434343", "434.3k"] {
            assert!(
                !svg.contains(magnitude),
                "{family:?} published the setup-bound magnitude {magnitude}"
            );
        }
        assert!(
            svg.contains("kafka: setup-bound"),
            "{family:?} must name the backend whose slice is setup-bound only"
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

    let svg =
        chartgen::render_to_string(&doc, Family::ParallelVsSequenced).expect("chart should render");
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
        let svg = chartgen::render_to_string(&doc, family).expect("chart should render");
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
    let axis_of = |doc: &Document| {
        let svg = chartgen::render_to_string(doc, Family::ThroughputVsConsumers)
            .expect("chart should render");
        // The y tick labels are the axis; extract them by their formatting.
        svg.match_indices("40k").count() + svg.match_indices("30k").count()
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
    // `autoscaler` is supported on every backend and is not in the fixture's
    // `unsupported[]`. Drawing an "n/s" column would claim a capability hole
    // that does not exist; dropping it without a word is the omission the
    // marker rule exists to prevent. It must be named in the caption.
    let svg = chartgen::render_to_string(
        &parse(&document(&inmemory_run(true))),
        Family::FrameworkOverhead,
    )
    .expect("chart should render");

    // Slice-scoped on purpose: "not measured in this run" was a false claim
    // for a flow measured at coordinates outside the chart's slice.
    assert!(
        svg.contains("no number in this slice"),
        "supported-but-unmeasured flows are dropped without a word"
    );
    assert!(
        svg.contains("autoscaler"),
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

    let svg =
        chartgen::render_to_string(&doc, Family::ThroughputVsConsumers).expect("should render");
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
            chartgen::render_to_string(&doc, Family::FrameworkOverhead),
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
        let first = chartgen::render_to_string(&doc, family).expect("first render");
        let second = chartgen::render_to_string(&doc, family).expect("second render");
        assert_eq!(first, second, "{family:?} is not byte-deterministic");
    }
}

#[test]
fn output_carries_no_wall_clock_and_no_external_reference() {
    // A generation timestamp would make the SVG differ on every run by
    // construction, and an external font or stylesheet silently drops under
    // raw.githubusercontent's `default-src 'none'` CSP.
    let doc = parse(&document(&inmemory_run(true)));
    for (family, svg) in render_all(&doc) {
        assert!(
            !svg.contains("2026-09") && !svg.contains("2026-10"),
            "{family:?} looks like it rendered a wall-clock date"
        );
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
fn charts_paint_no_background_and_no_near_black_ink() {
    // Theme neutrality is structural: an unpainted background lets the page
    // show through, and no fill may be white or near-black.
    let doc = parse(&document(&inmemory_run(true)));
    for (family, svg) in render_all(&doc) {
        for forbidden in [
            "#FFFFFF",
            "#ffffff",
            "#000000",
            "rgb(255,255,255)",
            "rgb(0,0,0)",
        ] {
            assert!(
                !svg.contains(forbidden),
                "{family:?} uses {forbidden}, which disappears in one of the two themes"
            );
        }
    }
}

// ── Layout: nothing may run off the canvas ──────────────────────────────────

/// Every `<text>` in the file, as (x, y, font-size, content).
fn texts(svg: &str) -> Vec<(f64, f64, f64, String)> {
    let mut out = Vec::new();
    for chunk in svg.split("<text ").skip(1) {
        let Some(end) = chunk.find("</text>") else {
            continue;
        };
        let (head, body) = chunk.split_at(chunk.find('>').unwrap_or(0));
        let attr = |name: &str| -> Option<String> {
            let key = format!("{name}=\"");
            let start = head.find(&key)? + key.len();
            let rest = head.get(start..)?;
            let stop = rest.find('"')?;
            rest.get(..stop).map(str::to_string)
        };
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
                (0.0..=560.0).contains(&y),
                "{family:?}: text baseline y={y} is outside the canvas: {content:?}"
            );
            // A start-anchored run of text must end inside the canvas. 0.55em
            // per character over-estimates a proportional face, so this is the
            // conservative direction.
            let width = content.chars().count() as f64 * size * 0.55;
            if x == 24.0 {
                assert!(
                    x + width <= 960.0,
                    "{family:?}: text runs to {:.0}px, past the 960px edge: {content:?}",
                    x + width
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
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers).expect("renders");

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
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers)
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
    let svg =
        chartgen::render_to_string(&doc, Family::ParallelVsSequenced).expect("chart should render");
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
    let svg =
        chartgen::render_to_string(&doc, Family::ParallelVsSequenced).expect("chart should render");
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
                    {{ "flow": "dlq_drain", "reason": "{filler}-i{i}" }},
                    {{ "flow": "autoscaler", "reason": "{filler}-j{i}" }}
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
    match chartgen::render_to_string(&doc, Family::ParallelVsSequenced) {
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
    // Series colours index into the shared palette; if a family's series list
    // outgrows it, palette_colour wraps deterministically rather than
    // silently reusing the first hue — but the fixed lists should simply fit.
    assert!(
        chartgen::PALETTE.len() >= 3,
        "families 3/4 use three series"
    );
    assert!(
        chartgen::PALETTE.len() >= 6,
        "the backend legend uses six colours"
    );
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
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers)
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
    assert!(
        !svg.contains("<rect") || !svg.contains("opacity=\"1\""),
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
    let svg = chartgen::render_to_string(&parse(&document(&run)), Family::FrameworkOverhead)
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
    // Six shape-only backends carry the longest legend labels this document
    // shape can produce; every entry must stay inside the 960px viewBox or a
    // plotted series has no legible label.
    let backends = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    let runs: Vec<String> = backends
        .iter()
        .map(|b| {
            format!(
                r#"{{
                  "backend": "{b}-backend-name", "representative": false,
                  "broker": {{ "name": "LocalStack", "version": "3", "deployment": "docker" }},
                  "results": [{}], "failures": [], "unsupported": []
                }}"#,
                scenario("consume_parallel", "parallel", 64, 1, 5_000.0)
            )
        })
        .collect();
    let doc = parse(&document(&format!(
        "{},{}",
        inmemory_run(true),
        runs.join(",")
    )));
    let svg = chartgen::render_to_string(&doc, Family::ThroughputVsConsumers)
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
    let svg =
        chartgen::render_to_string(&doc, Family::ParallelVsSequenced).expect("chart should render");
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
        scenario_with_cost(
            "consume_parallel",
            "parallel",
            1024,
            1,
            57_800.0,
            "setup_bound"
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
    let svg =
        chartgen::render_to_string(&doc, Family::ThroughputVsPayload).expect("chart should render");
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
        svg.contains("partial — setup-bound cells"),
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
        if let Ok(svg) = chartgen::render_to_string(&doc, family) {
            assert!(
                !svg.contains("NaN"),
                "{family:?} rendered NaN coordinates from a subnormal throughput"
            );
        }
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
        let svg = chartgen::render_to_string(&doc, family)
            .unwrap_or_else(|e| panic!("{family:?} should render from the committed doc: {e}"));
        assert!(svg.starts_with("<svg"), "{family:?} did not produce an SVG");
        assert!(
            svg.contains(&doc.generated_at),
            "{family:?} lost the provenance date"
        );
        // The committed SVGs must be exactly what this code renders from the
        // committed document — anyone who updates one half without the other
        // fails here, in `cargo test`, without waiting for a CI leg.
        let committed = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("docs/public/bench")
            .join(family.filename());
        let committed = std::fs::read_to_string(&committed)
            .unwrap_or_else(|e| panic!("read {}: {e}", committed.display()));
        assert!(
            svg == committed,
            "{family:?}: the committed SVG is not what the committed document renders — \
             regenerate with `cargo run --no-default-features --example chartgen -- \
             --input benches/results/bench-results.json --out-dir docs/public/bench`"
        );
    }
}
