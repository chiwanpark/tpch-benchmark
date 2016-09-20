package tpch.parser.validation

import tpch.parser.Parser

class Query04Parser extends Parser[(String, Int)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1) =>
        try {
          Some((p0, p1.toInt))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
