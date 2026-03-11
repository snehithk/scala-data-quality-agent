import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.io.Source

import ujson.Value

final case class Row(values: Map[String, String])

final case class DataIssue(
    issueType: String,
    column: String,
    severity: String,
    count: Int,
    description: String,
    evidence: ujson.Obj
) {
  def toJson: Value = ujson.Obj(
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
    params: ujson.Obj,
    priority: String,
    reason: String
) {
  def toJson: Value = ujson.Obj(
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

object ProjectDefaults {
  val AllowedMarkets = Seq("NASDAQ", "NYSE", "AMEX")
}

object Cli {
  def parseArgs(args: List[String]): Args = {
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

  def printUsage(): Unit = {
    println("Usage:")
    println("  scala-cli run src/main/scala/DataQualityAgentApp.scala -- --sample-data")
    println("  scala-cli run src/main/scala/DataQualityAgentApp.scala -- --input data.csv --expected-schema schema.json --output report.json")
  }
}

object DataSources {
  def loadRows(args: Args): Vector[Row] =
    if (args.sampleData) createSampleData()
    else readCsv(args.input.getOrElse(throw new IllegalArgumentException("Missing --input")))

  def readJson(path: Path): Value =
    ujson.read(Files.readString(path, StandardCharsets.UTF_8))

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
}
