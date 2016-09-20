package tpch.parser.validation

import tpch.parser.Parser

class Query02Parser extends Parser[(Double, String, String, Int, String, String, String, String)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2, p3, p4, p5, p6, p7) =>
        try {
          Some((p0.toDouble, p1, p2, p3.toInt, p4, p5, p6, p7))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
