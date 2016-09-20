package tpch.conf

class ValidateConfigurationParser extends scopt.OptionParser[ValidateConfiguration]("validate") {
  head("validate")

  opt[String]("answerPath")
    .action((x, c) => c.copy(answerPath = x))
    .required()
  opt[String]("resultPath")
    .action((x, c) => c.copy(resultPath = x))
    .required()
  opt[Int]("numQuery")
    .action((x, c) => c.copy(numQuery = x))
    .required()
}
