package tpch.parser.validation

import tpch.parser.Parser

class Query20Parser extends Parser[(String, String)] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(p0, p1) => Some((p0, p1))
      case _ => None
    }
  }
}
