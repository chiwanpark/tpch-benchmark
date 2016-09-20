package tpch.parser.validation

import tpch.parser.Parser

class Query07Parser extends Parser[(String, String, Int, Double)]{
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2, p3) =>
        try {
          Some((p0, p1, p2.toInt, p3.toDouble))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
