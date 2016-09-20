package tpch.parser.validation

import tpch.parser.Parser

class Query15Parser extends Parser[(Int, String, String, String, Double)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2, p3, p4) =>
        try {
          Some((p0.toInt, p1, p2, p3, p4.toDouble))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
