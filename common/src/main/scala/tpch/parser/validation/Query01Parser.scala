package tpch.parser.validation

import tpch.parser.Parser

class Query01Parser extends Parser[(String, String, Double, Double, Double, Double, Double, Double, Double, Int)]{
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1, p2, p3, p4, p5, p6, p7, p8, p9) =>
        try {
          Some((p0, p1, p2.toDouble, p3.toDouble, p4.toDouble, p5.toDouble, p6.toDouble, p7.toDouble, p8.toDouble,
            p9.toInt))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
