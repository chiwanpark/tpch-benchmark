package tpch.parser.validation

import tpch.parser.Parser

class Query03Parser extends Parser[(Int, Double, String, Int)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2, p3) =>
        try {
          Some((p0.toInt, p1.toDouble, p2, p3.toInt))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
