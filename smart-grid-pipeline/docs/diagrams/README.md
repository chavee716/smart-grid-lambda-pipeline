# Diagram generation

These PNGs are checked in for convenience (used directly by `report/`), but
are fully reproducible from code:

- `lambda_architecture.png` / `lambda_vs_kappa.png` — generated with
  Graphviz from the architecture description in the report. Regenerate with
  any Graphviz-based script describing the same node/edge structure shown
  in `report/EC8203_MiniProject_Report.pdf` Figures 1-2.
- `sample_realtime_dashboard.png` / `sample_billing_report.png` — generated
  by running the ACTUAL `simulators.smart_meter_streamer.Household` class
  and the ACTUAL `batch_processing.daily_billing_job.compute_daily_bills()`
  function over one simulated day, then plotting with matplotlib. This
  ties the report's "sample output" figures directly to the production
  code path rather than hand-mocked images. See report §9 for details.
