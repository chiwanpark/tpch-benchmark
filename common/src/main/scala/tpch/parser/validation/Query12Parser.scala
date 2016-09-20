package tpch.parser.validation

import tpch.parser.Parser

class Query12Parser extends Parser[(String, Int, Int)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2) =>
        try {
          Some((p0, p1.toInt, p2.toInt))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
