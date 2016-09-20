package tpch.parser.validation

import tpch.parser.Parser

class Query22Parser extends Parser[(Int, Int, Double)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2) =>
        try {
          Some((p0.toInt, p1.toInt, p2.toDouble))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
