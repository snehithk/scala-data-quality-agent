//> using scala 2.13.16
//> using dep com.lihaoyi::ujson:4.1.0

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Instant

import scala.io.Source
import scala.util.Try

import ujson._

object DataQualityAgentApp {

  final case class Row(values: Map[String, String])

  final case class DataIssue(
      issueType: String,
      column: String,
      severity: String,
      count: Int,
      description: String,
      evidence: Obj
  ) {
    def toJson: Value = Obj(
      "issue_type" -> issueType,
      "column" -> column,
      "severity" -> severity,
      "count" -> count,
      "description" -> description,
      "evidence" -> evidence
    )
  }

  final case class CheckSpec(
      name: String,
      params: Obj,
      priority: String,
      reason: String
  ) {
    def toJson: Value = Obj(
      "name" -> name,
      "params" -> params,
      "priority" -> priority,
      "reason" -> reason
    )
  }

  final case class Args(
      input: Option[Path] = None,
      expectedSchema: Option[Path] = None,
      output: Path = Paths.get("quality_report.json"),
      sampleData: Boolean = false,
      apiKey: Option[String] = None,
      model: String = "nvidia/nemotron-3-nano-30b-a3b:free"
  )

  val DefaultAllowedMarkets = Seq("NASDAQ", "NYSE", "AMEX")

  def main(rawArgs: Array[String]): Unit = {
    val args = parseArgs(rawArgs.toList)
    val rows = loadRows(args)
    val expectedSchema = args.expectedSchema.map(readJson)

    val agent = new DataQualityAgent(
      apiKey = args.apiKey.orElse(sys.env.get("OPENROUTER_API_KEY")),
      model = args.model
    )

    val analysis = agent.analyze(rows, expectedSchema)
    Files.writeString(args.output, ujson.write(analysis, indent = 2), StandardCharsets.UTF_8)
    println(agent.renderTextReport(analysis))
    println()
    println(s"Saved JSON report to ${args.output.toAbsolutePath.normalize()}")
  }

  private def parseArgs(args: List[String]): Args = {
    @annotation.tailrec
    def loop(rest: List[String], acc: Args): Args = rest match {
      case Nil => acc.copy(sampleData = acc.sampleData || acc.input.isEmpty)
      case "--input" :: value :: tail => loop(tail, acc.copy(input = Some(Paths.get(value))))
      case "--expected-schema" :: value :: tail => loop(tail, acc.copy(expectedSchema = Some(Paths.get(value))))
      case "--output" :: value :: tail => loop(tail, acc.copy(output = Paths.get(value)))
      case "--sample-data" :: tail => loop(tail, acc.copy(sampleData = true))
      case "--api-key" :: value :: tail => loop(tail, acc.copy(apiKey = Some(value)))
      case "--model" :: value :: tail => loop(tail, acc.copy(model = value))
      case "--help" :: _ =>
        printUsage()
        sys.exit(0)
      case unknown :: _ =>
        throw new IllegalArgumentException(s"Unknown argument: $unknown")
    }

    loop(args, Args())
  }

  private def printUsage(): Unit = {
    println("Usage:")
    println("  scala-cli run src/main/scala/DataQualityAgentApp.scala -- --sample-data")
    println("  scala-cli run src/main/scala/DataQualityAgentApp.scala -- --input data.csv --expected-schema schema.json --output report.json")
  }

  private def loadRows(args: Args): Vector[Row] =
    if (args.sampleData) createSampleData()
    else readCsv(args.input.getOrElse(throw new IllegalArgumentException("Missing --input")))

  private def readCsv(path: Path): Vector[Row] = {
    val lines = Source.fromFile(path.toFile).getLines().toVector.filter(_.trim.nonEmpty)
    if (lines.isEmpty) Vector.empty
    else {
      val headers = splitCsvLine(lines.head)
      lines.tail.map { line =>
        val values = splitCsvLine(line)
        Row(headers.zipAll(values, "", "").toMap)
      }
    }
  }

  private def splitCsvLine(line: String): Vector[String] =
    line.split(",", -1).toVector.map(_.trim.stripPrefix("\"").stripSuffix("\""))

  private def createSampleData(): Vector[Row] = {
    val base = (1 to 120).map { index =>
      val day = f"${((index - 1) % 28) + 1}%02d"
      val open = 150 + (index % 11) * 3.7
      val close = 151 + (index % 13) * 2.9
      val volume = 1000000 + index * 25000
      Row(
        Map(
          "date" -> s"2024-01-$day",
          "stock_symbol" -> "AAPL",
          "open_price" -> f"$open%.2f",
          "close_price" -> f"$close%.2f",
          "volume" -> volume.toString,
          "market" -> "NASDAQ"
        )
      )
    }.toVector

    base
      .updated(10, base(10).copy(values = base(10).values.updated("close_price", "")))
      .updated(20, base(20).copy(values = base(20).values.updated("volume", "-100")))
      .updated(30, base(30).copy(values = base(30).values.updated("open_price", "Infinity")))
      .updated(40, base(40).copy(values = base(40).values.updated("stock_symbol", "")))
      .updated(55, base(55).copy(values = base(55).values.updated("close_price", "4999.99")))
      .updated(60, base(60).copy(values = base(60).values.updated("market", "NASDQ"))) ++
      Vector(base(5), base(6))
  }

  private def readJson(path: Path): Value = ujson.read(Files.readString(path, StandardCharsets.UTF_8))
}

final class DataQualityAgent(apiKey: Option[String], model: String) {
  import DataQualityAgentApp._

  private val validatorRegistry: Map[String, CheckSpec => Vector[DataIssue]] = Map(
    "missing_values" -> runMissingValuesCheck,
    "duplicate_rows" -> runDuplicateRowsCheck,
    "infinite_values" -> runInfiniteValuesCheck,
    "blank_strings" -> runBlankStringsCheck,
    "outliers" -> runOutlierCheck,
    "schema_validation" -> runSchemaValidationCheck,
    "non_negative" -> runNonNegativeCheck,
    "allowed_values" -> runAllowedValuesCheck,
    "composite_uniqueness" -> runCompositeUniquenessCheck,
    "identifier_duplicates" -> runIdentifierDuplicateCheck
  )

  def analyze(rows: Vector[Row], expectedSchema: Option[Value]): Value = {
    currentRows = rows
    currentExpectedSchema = expectedSchema
    val profile = profileDataset(rows)
    val semanticContext = inferSemantics(rows, profile)
    val checkPlan = buildCheckPlan(semanticContext, expectedSchema)
    val execution = runChecks(rows, semanticContext, checkPlan, expectedSchema)
    val issues = execution("issues").arr.toVector.map(parseIssue)
    val score = computeScore(rows.size.max(1), issues)
    val assessment = assess(profile, semanticContext, checkPlan, issues, score)

    Obj(
      "generated_at" -> Instant.now().toString,
      "agent" -> Obj(
        "name" -> "Scala Data Quality Agent",
        "model" -> model,
        "used_llm" -> Bool(
          semanticContext("analysis_mode").str == "llm" || assessment("analysis_mode").str == "llm"
        )
      ),
      "dataset_profile" -> profile,
      "semantic_context" -> semanticContext,
      "check_plan" -> checkPlan,
      "executed_checks" -> execution("executed_checks"),
      "advisory_checks" -> execution("advisory_checks"),
      "issues" -> execution("issues"),
      "quality_score" -> score,
      "severity" -> assessment("severity"),
      "assessment" -> assessment
    )
  }

  def renderTextReport(analysis: Value): String = {
    val profile = analysis("dataset_profile")
    val semantic = analysis("semantic_context")
    val plan = analysis("check_plan")("checks").arr.take(6)
    val executedChecks = analysis("executed_checks").arr
    val advisoryChecks = analysis("advisory_checks").arr
    val issues = analysis("issues").arr.take(8)
    val assessment = analysis("assessment")

    val builder = new StringBuilder
    builder.append("=" * 80).append('\n')
    builder.append("SCALA DATA QUALITY AGENT\n")
    builder.append("=" * 80).append('\n')
    builder.append(s"Generated: ${analysis("generated_at").str}\n")
    builder.append(s"Rows: ${profile("row_count").num.toInt} | Columns: ${profile("column_count").num.toInt}\n")
    builder.append(s"Quality Score: ${analysis("quality_score").num.toInt} (${analysis("severity").str})\n")
    builder.append(s"Semantic Inference Mode: ${semantic("analysis_mode").str}\n")
    builder.append(s"Assessment Mode: ${assessment("analysis_mode").str}\n")
    builder.append(s"Dataset Type: ${semantic("dataset_type").str}\n")
    builder.append(s"Dataset Purpose: ${semantic("dataset_purpose").str}\n\n")
    builder.append("Planned Checks:\n")
    plan.foreach { check =>
      builder.append(s"- [${check("priority").str}] ${check("name").str}: ${check("reason").str}\n")
    }
    builder.append(s"\nExecuted Checks: ${executedChecks.size}\n")
    if (advisoryChecks.nonEmpty) {
      builder.append("Advisory Checks:\n")
      advisoryChecks.take(4).foreach { check =>
        builder.append(s"- ${check("name").str}: ${check("reason").str}\n")
      }
    }
    builder.append("\nKey Issues:\n")
    issues.foreach { issue =>
      builder.append(s"- [${issue("severity").str}] ${issue("issue_type").str} in ${issue("column").str}: ${issue("description").str}\n")
    }
    builder.append("\nExecutive Summary:\n")
    builder.append(assessment("executive_summary").str).append('\n')
    builder.append("\nRecommendations:\n")
    assessment("recommendations").arr.foreach(item => builder.append(s"- ${item.str}\n"))
    builder.toString()
  }

  private def profileDataset(rows: Vector[Row]): Value = {
    val columns = rows.headOption.map(_.values.keys.toVector).getOrElse(Vector.empty)
    val missingByColumn = columns.flatMap { column =>
      val count = rows.count(row => row.values.getOrElse(column, "").trim.isEmpty)
      Option.when(count > 0)(column -> Num(count))
    }

    val duplicateRows = rows.groupBy(_.values).values.count(_.size > 1)
    val numericColumns = columns.filter(column => rows.exists(r => parseDouble(r.values.getOrElse(column, "")).isDefined))

    val numericSummary = Obj.from(numericColumns.map { column =>
      val values = rows.flatMap(r => parseDouble(r.values.getOrElse(column, ""))).filter(_.isFinite)
      val sorted = values.sorted
      val mean = if (values.nonEmpty) values.sum / values.size else 0.0
      val median = if (sorted.isEmpty) 0.0 else sorted(sorted.size / 2)
      val std = if (values.nonEmpty) math.sqrt(values.map(v => math.pow(v - mean, 2)).sum / values.size) else 0.0
      column -> Obj(
        "mean" -> Num(round4(mean)),
        "median" -> Num(round4(median)),
        "std" -> Num(round4(std)),
        "min" -> Num(round4(values.minOption.getOrElse(0.0))),
        "max" -> Num(round4(values.maxOption.getOrElse(0.0)))
      )
    })

    val categoricalColumns = columns.filterNot(numericColumns.contains)
    val categoricalSummary = Obj.from(categoricalColumns.map { column =>
      val counts = rows.groupBy(_.values.getOrElse(column, "")).view.mapValues(_.size).toMap.toSeq.sortBy(-_._2).take(5)
      column -> Obj(
        "unique_values" -> Num(rows.map(_.values.getOrElse(column, "")).distinct.count(_.nonEmpty)),
        "top_values" -> Obj.from(counts.map { case (value, count) => value -> Num(count) })
      )
    })

    val sampleRows = Arr.from(rows.take(5).map(row => Obj.from(row.values.toSeq.map { case (k, v) => k -> Str(v) })))

    Obj(
      "row_count" -> Num(rows.size),
      "column_count" -> Num(columns.size),
      "columns" -> Arr.from(columns.map(Str(_))),
      "dtypes" -> Obj.from(columns.map { column =>
        column -> Str(inferType(rows.flatMap(r => Option(r.values.getOrElse(column, "")).filter(_.nonEmpty))))
      }),
      "missing_by_column" -> Obj.from(missingByColumn),
      "duplicate_rows" -> Num(duplicateRows),
      "numeric_summary" -> numericSummary,
      "categorical_summary" -> categoricalSummary,
      "sample_rows" -> sampleRows
    )
  }

  private def inferSemantics(rows: Vector[Row], profile: Value): Value =
    apiKey match {
      case Some(key) => callSemanticInference(profile, key).getOrElse(heuristicSemantics(profile))
      case None => heuristicSemantics(profile)
    }

  private def buildCheckPlan(semantic: Value, expectedSchema: Option[Value]): Value = {
    val baseChecks = Vector(
      CheckSpec("missing_values", Obj(), "HIGH", "Completeness is a baseline quality gate."),
      CheckSpec("duplicate_rows", Obj(), "HIGH", "Duplicate records distort downstream metrics."),
      CheckSpec("infinite_values", Obj(), "HIGH", "Infinite values break analytics and models."),
      CheckSpec("blank_strings", Obj(), "MEDIUM", "Blank strings hide missing data."),
      CheckSpec("outliers", Obj(), "MEDIUM", "Extreme values may indicate ingestion issues.")
    )

    val schemaCheck = expectedSchema.toVector.map(_ =>
      CheckSpec("schema_validation", Obj(), "HIGH", "An expected schema was supplied by the caller.")
    )

    val inferredChecks = deriveChecksFromSemantics(semantic)
    val aiChecks = semantic.obj.get("priority_checks").map(_.arr.toVector.flatMap(normalizeCheck)).getOrElse(Vector.empty)
    val checks = dedupeChecks(baseChecks ++ schemaCheck ++ inferredChecks ++ aiChecks)

    val columnRules = Obj()
    semantic("column_semantics").obj.foreach { case (column, value) =>
      normalizeColumnSemantic(column, value).foreach { normalized =>
        val rule = Obj()
        normalized.value.get("should_be_non_negative").foreach(v => rule("non_negative") = v)
        normalized.value.get("allowed_values").filterNot(_.isNull).foreach(v => rule("allowed_values") = v)
        normalized.value.get("is_identifier").foreach(v => rule("identifier") = v)
        if (rule.obj.nonEmpty) columnRules(column) = rule
      }
    }

    Obj(
      "analysis_mode" -> semantic("analysis_mode").str,
      "dataset_purpose" -> semantic("dataset_purpose").str,
      "business_rules" -> semantic("business_rules"),
      "checks" -> Arr.from(checks.map(_.toJson)),
      "column_rules" -> columnRules
    )
  }

  private def runChecks(
      rows: Vector[Row],
      semantic: Value,
      plan: Value,
      expectedSchema: Option[Value]
  ): Value = {
    val planSpecs = plan("checks").arr.toVector.flatMap(parseCheckSpec)
    val supported = collection.mutable.ArrayBuffer.empty[Value]
    val advisory = collection.mutable.ArrayBuffer.empty[Value]
    val allIssues = collection.mutable.ArrayBuffer.empty[DataIssue]

    planSpecs.foreach { spec =>
      validatorRegistry.get(spec.name) match {
        case Some(validator) =>
          supported += spec.toJson
          allIssues ++= validator(spec)
        case None =>
          advisory += spec.toJson
      }
    }

    val dedupedIssues = dedupeIssues(allIssues.toVector).sortBy(issue => (severityRank(issue.severity), issue.issueType, issue.column))
    Obj(
      "executed_checks" -> Arr.from(supported),
      "advisory_checks" -> Arr.from(advisory),
      "issues" -> Arr.from(dedupedIssues.map(_.toJson))
    )
  }

  private def computeScore(rowCount: Int, issues: Vector[DataIssue]): Int = {
    val penalties = Map("HIGH" -> 8.0, "MEDIUM" -> 4.0, "LOW" -> 2.0)
    val score = issues.foldLeft(100.0) { (acc, issue) =>
      val base = penalties.getOrElse(issue.severity, 2.0)
      val ratio = math.min((issue.count.toDouble / rowCount) * 20.0, 10.0)
      acc - base - ratio
    }
    math.max(0, math.min(100, math.round(score).toInt))
  }

  private def assess(
      profile: Value,
      semantic: Value,
      plan: Value,
      issues: Vector[DataIssue],
      score: Int
  ): Value =
    apiKey match {
      case Some(key) => callAssessment(profile, semantic, plan, issues, score, key).getOrElse(heuristicAssessment(profile, semantic, plan, issues, score))
      case None => heuristicAssessment(profile, semantic, plan, issues, score)
    }

  private def heuristicSemantics(profile: Value): Value = {
    val columns = profile("columns").arr.map(_.str)
    val semantics = Obj.from(columns.map { column =>
      val lowered = column.toLowerCase
      val semanticType =
        if (lowered.contains("date")) "event_date"
        else if (lowered.contains("symbol")) "ticker_symbol"
        else if (lowered.contains("market")) "exchange_code"
        else if (Seq("price", "volume", "count", "amount", "qty").exists(lowered.contains)) "metric"
        else "generic_field"

      val allowedValues: Value =
        if (lowered.contains("market")) Arr.from(DefaultAllowedMarkets.map(Str(_)))
        else Null

      column -> Obj(
        "semantic_type" -> semanticType,
        "should_be_non_negative" -> Bool(semanticType == "metric"),
        "is_identifier" -> Bool(lowered == "id" || lowered.endsWith("_id")),
        "allowed_values" -> allowedValues
      )
    })

    Obj(
      "analysis_mode" -> "heuristic",
      "dataset_purpose" -> "Tabular data quality monitoring",
      "dataset_type" -> "tabular_dataset",
      "business_rules" -> Arr(
        "Important metric columns should remain non-negative.",
        "Categorical codes should stay within expected domains."
      ),
      "column_semantics" -> semantics,
      "priority_checks" -> Arr(
        Obj("name" -> "non_negative", "params" -> Obj("column" -> "price"), "priority" -> "HIGH", "reason" -> "Metrics should not be negative."),
        Obj("name" -> "allowed_values", "params" -> Obj("column" -> "market", "values" -> Arr.from(DefaultAllowedMarkets.map(Str(_)))), "priority" -> "MEDIUM", "reason" -> "Code columns should stay within expected values.")
      )
    )
  }

  private def heuristicAssessment(
      profile: Value,
      semantic: Value,
      plan: Value,
      issues: Vector[DataIssue],
      score: Int
  ): Value = {
    val severity = scoreToSeverity(score)
    Obj(
      "analysis_mode" -> "heuristic",
      "severity" -> severity,
      "executive_summary" -> s"The agent classified this dataset as ${semantic("dataset_type").str}. It executed ${plan("checks").arr.size} planned checks, found ${issues.size} issue(s), and assigned a quality score of $score/100.",
      "key_issues" -> Arr.from(issues.take(5).map(issue => Str(issue.description))),
      "root_causes" -> Arr(
        "Upstream validation is not enforcing required fields and domain rules.",
        "The ingestion path does not remove duplicate or malformed records."
      ),
      "recommendations" -> Arr(
        "Add validation before loading the dataset into downstream tables.",
        "Track quality reports over time to detect regressions.",
        "Use the generated issues list as the deterministic source of truth for pipeline gating."
      ),
      "next_checks" -> Arr(
        "Compare current statistics with previous runs to detect drift.",
        "Add dataset-specific business rules for identifiers and ranges."
      ),
      "suggested_fixes" -> Arr(
        "Remove duplicate rows before publishing the dataset.",
        "Replace invalid numeric values with null and quarantine affected rows.",
        "Enforce categorical reference data before load."
      )
    )
  }

  private def callSemanticInference(profile: Value, key: String): Option[Value] = {
    val prompt =
      s"""You are an AI data quality agent. Infer what this dataset likely represents and which checks matter most.
         |Return valid JSON only with keys:
         |dataset_purpose, dataset_type, business_rules, column_semantics, priority_checks.
         |For priority_checks, each item must be an object with keys: name, params, priority, reason.
         |Use supported check names when possible: non_negative, allowed_values, composite_uniqueness, identifier_duplicates, outliers.
         |
         |Dataset payload:
         |${ujson.write(profile, indent = 2)}
         |""".stripMargin

    callOpenRouter(prompt, key).flatMap { raw =>
      parseJsonObject(raw, Set("dataset_purpose", "dataset_type", "business_rules", "column_semantics", "priority_checks"))
        .map { obj =>
          obj("analysis_mode") = Str("llm")
          obj
        }
    }
  }

  private def callAssessment(
      profile: Value,
      semantic: Value,
      plan: Value,
      issues: Vector[DataIssue],
      score: Int,
      key: String
  ): Option[Value] = {
    val payload = Obj(
      "quality_score" -> score,
      "profile" -> profile,
      "semantic_context" -> semantic,
      "check_plan" -> plan,
      "issues" -> Arr.from(issues.map(_.toJson))
    )

    val prompt =
      s"""You are an AI data quality agent reviewing a validation run.
         |Return valid JSON only with keys:
         |severity, executive_summary, key_issues, root_causes, recommendations, next_checks, suggested_fixes.
         |
         |Analysis payload:
         |${ujson.write(payload, indent = 2)}
         |""".stripMargin

    callOpenRouter(prompt, key).flatMap { raw =>
      parseJsonObject(raw, Set("severity", "executive_summary", "key_issues", "root_causes", "recommendations", "next_checks", "suggested_fixes"))
        .map { obj =>
          normalizeAssessment(obj)
          obj("analysis_mode") = Str("llm")
          obj
        }
    }
  }

  private def callOpenRouter(prompt: String, key: String): Option[String] = Try {
    val payload = Obj(
      "model" -> model,
      "messages" -> Arr(Obj("role" -> "user", "content" -> prompt)),
      "temperature" -> 0.2,
      "response_format" -> Obj("type" -> "json_object")
    )

    val request = HttpRequest.newBuilder()
      .uri(URI.create("https://openrouter.ai/api/v1/chat/completions"))
      .header("Authorization", s"Bearer $key")
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(ujson.write(payload)))
      .build()

    val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      val json = ujson.read(response.body())
      json("choices")(0)("message")("content").str
    } else throw new RuntimeException(response.body())
  }.toOption

  private def parseJsonObject(raw: String, requiredKeys: Set[String]): Option[Obj] =
    Try(ujson.read(raw)).toOption.collect {
      case obj: Obj if requiredKeys.subsetOf(obj.value.keySet) => obj
    }

  private def normalizeAssessment(obj: Obj): Unit = {
    Seq("key_issues", "root_causes", "recommendations", "next_checks", "suggested_fixes").foreach { key =>
      obj.value.get(key).foreach {
        case arr: Arr => obj(key) = arr
        case Str(text) =>
          obj(key) = Arr.from(text.split("\n").toVector.map(_.trim).filter(_.nonEmpty).map(Str(_)))
        case other =>
          obj(key) = Arr(Str(other.render()))
      }
    }
  }

  private def validateSchema(rows: Vector[Row], schema: Value): Vector[DataIssue] = {
    val rules = schema.obj.toVector
    rules.flatMap { case (column, rawRules) =>
      val ruleObj = rawRules.obj
      if (!rows.headOption.exists(_.values.contains(column))) {
        Vector(DataIssue("MISSING_COLUMN", column, "HIGH", 1, s"Expected column '$column' is missing from the dataset.", Obj()))
      } else {
        val values = rows.map(_.values.getOrElse(column, ""))
        val dtypeIssues = ruleObj.get("dtype").toVector.flatMap { expected =>
          val actual = inferType(values.filter(_.nonEmpty))
          Option.when(!dtypeMatches(actual, expected.str))(
            DataIssue("SCHEMA_MISMATCH", column, "HIGH", 1, s"Column '$column' has dtype '$actual' but expected '${expected.str}'.", Obj("actual_dtype" -> actual, "expected_dtype" -> expected.str))
          )
        }

        val nullableIssues =
          if (ruleObj.get("nullable").exists(v => !v.bool)) {
            val nullCount = values.count(_.trim.isEmpty)
            Option.when(nullCount > 0)(DataIssue("NON_NULL_VIOLATION", column, "HIGH", nullCount, s"Column '$column' is required but contains blank or null values.", Obj())).toVector
          } else Vector.empty

        val enumIssues = ruleObj.get("allowed_values").toVector.flatMap { allowed =>
          val allowedSet = allowed.arr.map(_.str).toSet
          val invalid = values.filter(v => v.nonEmpty && !allowedSet.contains(v))
          Option.when(invalid.nonEmpty)(
            DataIssue("ENUM_VIOLATION", column, "MEDIUM", invalid.size, s"Column '$column' contains values outside the allowed set.", Obj("sample_invalid_values" -> Arr.from(invalid.distinct.take(5).map(Str(_)))))
          )
        }

        dtypeIssues ++ nullableIssues ++ enumIssues
      }
    }
  }

  private def dedupeChecks(checks: Vector[CheckSpec]): Vector[CheckSpec] =
    checks
      .groupBy(spec => (spec.name, ujson.write(spec.params)))
      .values
      .map(_.head)
      .toVector
      .sortBy(spec => (severityRank(spec.priority.toUpperCase), spec.name))

  private def normalizeCheck(value: Value): Option[CheckSpec] = value match {
    case obj: Obj =>
      val nameOpt =
        obj.value.get("name").collect { case Str(value) if value.trim.nonEmpty => value.trim }
          .orElse(obj.value.get("check_name").collect { case Str(value) if value.trim.nonEmpty => value.trim })

      nameOpt.map { name =>
        val priority = obj.value.get("priority").collect { case Str(value) if value.trim.nonEmpty => value.trim.toUpperCase }.getOrElse("MEDIUM")
        val reason = obj.value.get("reason").collect { case Str(value) if value.trim.nonEmpty => value.trim }
          .orElse(obj.value.get("description").collect { case Str(value) if value.trim.nonEmpty => value.trim })
          .getOrElse("AI-suggested dataset-specific check.")
        val params = obj.value.get("params").collect { case paramsObj: Obj => paramsObj }.getOrElse(Obj())

        CheckSpec(
          name = name.trim.toLowerCase.replaceAll("\\s+", "_"),
          params = params,
          priority = priority,
          reason = reason
        )
      }
    case Str(value) if value.trim.nonEmpty =>
      Some(CheckSpec(value.trim.toLowerCase.replaceAll("\\s+", "_"), Obj(), "MEDIUM", value.trim))
    case _ => None
  }

  private def parseCheckSpec(value: Value): Option[CheckSpec] = normalizeCheck(value)

  private def normalizeColumnSemantic(column: String, value: Value): Option[Obj] = value match {
    case obj: Obj => Some(obj)
    case Str(text) =>
      val lowered = text.toLowerCase
      val semanticType =
        if (lowered.contains("date")) "event_date"
        else if (lowered.contains("market") || lowered.contains("exchange")) "exchange_code"
        else if (lowered.contains("symbol") || lowered.contains("ticker")) "ticker_symbol"
        else if (Seq("price", "amount", "volume", "count", "qty").exists(lowered.contains)) "metric"
        else "generic_field"

      val allowedValues: Value =
        if (semanticType == "exchange_code") Arr.from(DataQualityAgentApp.DefaultAllowedMarkets.map(Str(_)))
        else Null

      Some(
        Obj(
          "semantic_type" -> semanticType,
          "should_be_non_negative" -> Bool(semanticType == "metric"),
          "is_identifier" -> Bool(column == "id" || column.endsWith("_id")),
          "allowed_values" -> allowedValues,
          "source_description" -> text
        )
      )
    case _ => None
  }

  private def dedupeIssues(issues: Vector[DataIssue]): Vector[DataIssue] =
    issues.groupBy(issue => (issue.issueType, issue.column, issue.description)).values.map(_.head).toVector

  private def deriveChecksFromSemantics(semantic: Value): Vector[CheckSpec] =
    semantic("column_semantics").obj.toVector.flatMap { case (column, value) =>
      normalizeColumnSemantic(column, value).toVector.flatMap { normalized =>
        val checks = collection.mutable.ArrayBuffer.empty[CheckSpec]
        if (normalized.value.get("should_be_non_negative").exists(_.bool)) {
          checks += CheckSpec("non_negative", Obj("column" -> column), "HIGH", s"Column '$column' should not contain negative values.")
        }
        normalized.value.get("allowed_values").filterNot(_.isNull).foreach { values =>
          checks += CheckSpec("allowed_values", Obj("column" -> column, "values" -> values), "MEDIUM", s"Column '$column' should stay within allowed values.")
        }
        if (normalized.value.get("is_identifier").exists(_.bool)) {
          checks += CheckSpec("identifier_duplicates", Obj("column" -> column), "HIGH", s"Identifier-like column '$column' should not repeat.")
        }
        checks.toVector
      }
    }

  private def parseIssue(value: Value): DataIssue =
    DataIssue(
      issueType = value("issue_type").str,
      column = value("column").str,
      severity = value("severity").str,
      count = value("count").num.toInt,
      description = value("description").str,
      evidence = value("evidence").obj
    )

  private def runMissingValuesCheck(spec: CheckSpec): Vector[DataIssue] = {
    val columns = datasetColumns
    columns.flatMap { column =>
      val count = currentRows.count(_.values.getOrElse(column, "").trim.isEmpty)
      Option.when(count > 0)(
        DataIssue("MISSING_VALUES", column, "HIGH", count, s"Column '$column' contains $count missing value(s).", Obj("missing_ratio" -> Num(round4(count.toDouble / currentRows.size.max(1)))))
      )
    }
  }

  private def runDuplicateRowsCheck(spec: CheckSpec): Vector[DataIssue] = {
    val duplicateCount = currentRows.groupBy(_.values).values.map(_.size - 1).filter(_ > 0).sum
    Option.when(duplicateCount > 0)(
      DataIssue("DUPLICATE_ROWS", "__row__", "HIGH", duplicateCount, s"Dataset contains $duplicateCount duplicated row(s).", Obj())
    ).toVector
  }

  private def runInfiniteValuesCheck(spec: CheckSpec): Vector[DataIssue] =
    datasetColumns.flatMap { column =>
      val values = currentRows.map(_.values.getOrElse(column, ""))
      val infiniteCount = values.count(isInfinity)
      Option.when(infiniteCount > 0)(
        DataIssue("INFINITE_VALUES", column, "HIGH", infiniteCount, s"Column '$column' contains $infiniteCount infinite value(s).", Obj())
      )
    }

  private def runBlankStringsCheck(spec: CheckSpec): Vector[DataIssue] =
    datasetColumns.flatMap { column =>
      val blankCount = currentRows.map(_.values.getOrElse(column, "")).count(_.trim.isEmpty)
      Option.when(blankCount > 0)(
        DataIssue("EMPTY_STRINGS", column, "MEDIUM", blankCount, s"Column '$column' contains $blankCount blank string value(s).", Obj())
      )
    }

  private def runOutlierCheck(spec: CheckSpec): Vector[DataIssue] = {
    val targetColumns = spec.params.value.get("column").map(v => Vector(v.str)).getOrElse(numericColumns)
    targetColumns.flatMap { column =>
      val values = numericValues(column)
      val outlierCount = countOutliers(values)
      Option.when(outlierCount > 0)(
        DataIssue("OUTLIERS", column, "MEDIUM", outlierCount, s"Column '$column' has $outlierCount value(s) outside the IQR bounds.", Obj())
      )
    }
  }

  private def runSchemaValidationCheck(spec: CheckSpec): Vector[DataIssue] =
    currentExpectedSchema.map(validateSchema(currentRows, _)).getOrElse(Vector.empty)

  private def runNonNegativeCheck(spec: CheckSpec): Vector[DataIssue] = {
    spec.params.value.get("column").toVector.flatMap { columnValue =>
      val column = columnValue.str
      val values = numericValues(column)
      val negativeCount = values.count(_ < 0)
      Option.when(negativeCount > 0)(
        DataIssue("NEGATIVE_VALUES", column, "MEDIUM", negativeCount, s"Column '$column' has $negativeCount unexpected negative value(s).", Obj("min_value" -> Num(values.min)))
      )
    }
  }

  private def runAllowedValuesCheck(spec: CheckSpec): Vector[DataIssue] = {
    val columnOpt = spec.params.value.get("column").collect { case Str(value) => value }
    val valuesOpt = spec.params.value.get("values").collect { case arr: Arr => arr.arr.map(_.str).toSet }
    (for {
      column <- columnOpt.toVector
      allowed <- valuesOpt.toVector
    } yield {
      val invalid = currentRows.map(_.values.getOrElse(column, "")).filter(v => v.nonEmpty && !allowed.contains(v))
      Option.when(invalid.nonEmpty)(
        DataIssue("ENUM_VIOLATION", column, "MEDIUM", invalid.size, s"Column '$column' contains values outside the allowed set.", Obj("sample_invalid_values" -> Arr.from(invalid.distinct.take(5).map(Str(_)))))
      )
    }).flatten
  }

  private def runCompositeUniquenessCheck(spec: CheckSpec): Vector[DataIssue] = {
    val columns = spec.params.value.get("columns").collect { case arr: Arr => arr.arr.map(_.str).toVector }.getOrElse(Vector.empty)
    if (columns.isEmpty) Vector.empty
    else {
      val duplicates = currentRows
        .map(row => columns.map(col => row.values.getOrElse(col, "")).mkString("||"))
        .filter(_.nonEmpty)
        .groupBy(identity)
        .values
        .map(_.size - 1)
        .filter(_ > 0)
        .sum
      Option.when(duplicates > 0)(
        DataIssue("COMPOSITE_DUPLICATES", columns.mkString(","), "HIGH", duplicates, s"Columns ${columns.mkString("(", ", ", ")")} contain duplicate combinations.", Obj())
      ).toVector
    }
  }

  private def runIdentifierDuplicateCheck(spec: CheckSpec): Vector[DataIssue] = {
    spec.params.value.get("column").toVector.flatMap { columnValue =>
      val column = columnValue.str
      val duplicates = currentRows.map(_.values.getOrElse(column, "")).filter(_.nonEmpty).groupBy(identity).values.map(_.size - 1).filter(_ > 0).sum
      Option.when(duplicates > 0)(
        DataIssue("IDENTIFIER_DUPLICATES", column, "HIGH", duplicates, s"Identifier-like column '$column' contains duplicate values.", Obj())
      )
    }
  }

  private var currentRows: Vector[Row] = Vector.empty
  private var currentExpectedSchema: Option[Value] = None

  private def datasetColumns: Vector[String] =
    currentRows.headOption.map(_.values.keys.toVector).getOrElse(Vector.empty)

  private def numericColumns: Vector[String] =
    datasetColumns.filter(column => currentRows.exists(row => parseDouble(row.values.getOrElse(column, "")).isDefined || isInfinity(row.values.getOrElse(column, ""))))

  private def numericValues(column: String): Vector[Double] =
    currentRows.map(_.values.getOrElse(column, "")).flatMap(parseDouble).filter(_.isFinite)

  private def countOutliers(values: Vector[Double]): Int = {
    if (values.size < 4) 0
    else {
      val sorted = values.sorted
      val q1 = sorted(sorted.size / 4)
      val q3 = sorted((sorted.size * 3) / 4)
      val iqr = q3 - q1
      if (iqr == 0) 0
      else {
        val lower = q1 - 1.5 * iqr
        val upper = q3 + 1.5 * iqr
        values.count(v => v < lower || v > upper)
      }
    }
  }

  private def parseDouble(value: String): Option[Double] =
    Try(value.trim.toDouble).toOption

  private def isInfinity(value: String): Boolean = {
    val lowered = value.trim.toLowerCase
    lowered == "inf" || lowered == "infinity" || lowered == "-inf" || lowered == "-infinity"
  }

  private def inferType(values: Seq[String]): String = {
    val nonEmpty = values.filter(_.nonEmpty)
    if (nonEmpty.isEmpty) "string"
    else if (nonEmpty.forall(v => v.matches("""\d{4}-\d{2}-\d{2}"""))) "date"
    else if (nonEmpty.forall(v => parseDouble(v).isDefined || isInfinity(v))) "double"
    else "string"
  }

  private def dtypeMatches(actual: String, expected: String): Boolean = {
    val aliases = Map(
      "object" -> Set("string"),
      "string" -> Set("string"),
      "str" -> Set("string"),
      "float" -> Set("double"),
      "double" -> Set("double"),
      "int" -> Set("double"),
      "date" -> Set("date")
    )
    aliases.getOrElse(expected.toLowerCase, Set(expected.toLowerCase)).contains(actual.toLowerCase)
  }

  private def round4(value: Double): Double =
    BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

  private def severityRank(value: String): Int =
    value.toUpperCase match {
      case "CRITICAL" => 0
      case "HIGH" => 1
      case "MEDIUM" => 2
      case "LOW" => 3
      case _ => 4
    }

  private def scoreToSeverity(score: Int): String =
    if (score >= 90) "LOW"
    else if (score >= 75) "MEDIUM"
    else if (score >= 50) "HIGH"
    else "CRITICAL"
}
