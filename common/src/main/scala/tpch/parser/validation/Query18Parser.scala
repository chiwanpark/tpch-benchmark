package tpch.parser.validation

import tpch.parser.Parser

class Query18Parser extends Parser[(String, Int, Int, String, Double, Double)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2, p3, p4, p5) =>
        try {
          Some((p0, p1.toInt, p2.toInt, p3, p4.toDouble, p5.toDouble))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
