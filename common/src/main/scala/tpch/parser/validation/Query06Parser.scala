package tpch.parser.validation

import tpch.parser.Parser

class Query06Parser extends Parser[Tuple1[Double]] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0) =>
        try {
          Some(Tuple1(p0.toDouble))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
