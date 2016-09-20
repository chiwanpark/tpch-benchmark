package tpch.parser.validation

import tpch.parser.Parser

class Query13Parser extends Parser[(Int, Int)]{
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1) =>
        try {
          Some((p0.toInt, p1.toInt))
        } catch {
          case _: NumberFormatException => None
        }
      case _ => None
    }
  }
}
