package tpch.conf

case class Configuration(
  dataPath: String = "",
  resultPath: Option[String] = None,
  numQuery: Int = -1)
