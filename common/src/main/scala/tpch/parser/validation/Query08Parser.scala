package tpch.parser.validation

import tpch.parser.Parser

class Query08Parser extends Parser[(Int, Double)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1) =>
        try {
          Some((p0.toInt, p1.toDouble))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
