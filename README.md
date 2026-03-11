# Scala Data Quality Agent

Small Scala CLI that profiles tabular data, runs deterministic quality checks, and can call OpenRouter for semantic inference and assessment.

This project uses `scala-cli`, so it can run without an `sbt` build setup.

## Run

Built-in sample dataset:

```bash
scala-cli run src/main/scala/DataQualityAgentApp.scala -- --sample-data --expected-schema examples/expected_schema.json
```

Real public stock dataset included in the repo:

```bash
scala-cli run src/main/scala/DataQualityAgentApp.scala -- \
  --input data/stocks.csv \
  --expected-schema data/stocks_schema.json \
  --output report.json
```

With your own CSV:

```bash
export OPENROUTER_API_KEY="your_key_here"
scala-cli run src/main/scala/DataQualityAgentApp.scala -- \
  --input data.csv \
  --expected-schema examples/expected_schema.json \
  --output report.json
```

## Current behavior

- profiles rows and columns
- detects missing values, duplicates, blank strings, infinities, negatives, and outliers
- validates schema and allowed values from JSON
- infers dataset semantics
- writes a JSON report and prints a text report
- includes a real stock-price dataset in `data/stocks.csv`

## GitHub Actions

The repository includes a workflow at `.github/workflows/scala-agent.yml` that:

- installs Java
- installs `scala-cli`
- runs the included real dataset
- uploads `quality_report.json` as a build artifact

If you want the workflow to call OpenRouter, add a repository secret named `OPENROUTER_API_KEY`.

## Files

- `src/main/scala/DataQualityAgentApp.scala`
- `data/stocks.csv`
- `data/stocks_schema.json`
- `examples/expected_schema.json`
- `README.md`
