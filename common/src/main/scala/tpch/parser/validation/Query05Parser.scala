package tpch.parser.validation

import tpch.parser.Parser

class Query05Parser extends Parser[(String, Double)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1) =>
        try {
          Some((p0, p1.toDouble))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
