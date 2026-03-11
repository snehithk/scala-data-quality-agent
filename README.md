# Scala Data Quality Agent

Small Scala CLI that profiles tabular data, runs deterministic quality checks, and can call OpenRouter for semantic inference and assessment.

This project uses `scala-cli`, so it can run without an `sbt` build setup.

## Run

Sample dataset:

```bash
scala-cli run src/main/scala/DataQualityAgentApp.scala -- --sample-data --expected-schema examples/expected_schema.json
```

With a CSV:

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

## GitHub Actions

The repository includes a workflow at `.github/workflows/scala-agent.yml` that:

- installs Java
- installs `scala-cli`
- runs the sample dataset
- uploads `quality_report.json` as a build artifact

## Files

- `src/main/scala/DataQualityAgentApp.scala`
- `examples/expected_schema.json`
- `README.md`
