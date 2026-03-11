//> using scala 2.13.16
//> using dep com.lihaoyi::ujson:4.1.0

import java.nio.charset.StandardCharsets
import java.nio.file.Files

object DataQualityAgentApp {
  def main(rawArgs: Array[String]): Unit = {
    val args = Cli.parseArgs(rawArgs.toList)
    val rows = DataSources.loadRows(args)
    val expectedSchema = args.expectedSchema.map(DataSources.readJson)

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
}
