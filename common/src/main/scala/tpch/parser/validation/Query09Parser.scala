package tpch.parser.validation

import tpch.parser.Parser

class Query09Parser extends Parser[(String, Int, Double)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2) =>
        try {
          Some((p0, p1.toInt, p2.toDouble))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
