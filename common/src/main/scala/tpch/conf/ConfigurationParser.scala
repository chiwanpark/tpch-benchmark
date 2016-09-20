package tpch.conf

class ConfigurationParser extends scopt.OptionParser[Configuration]("tpch") {
  head("tpch")

  opt[String]("dataPath").abbr("d")
    .action((x, c) => c.copy(dataPath = x))
    .required()
  opt[String]("resultPath").abbr("o")
    .action((x, c) => c.copy(resultPath = Some(x)))
  opt[Int]("numQuery").abbr("n")
    .action((x, c) => c.copy(numQuery = x))
    .required()
}
