package tpch.parser

import tpch.Schema.Nation

class NationParser extends Parser[Nation] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(nationKey, name, regionKey, comment) =>
        Some(Nation(nationKey.toInt, name, regionKey.toInt, comment))
      case _ => None
    }
  }
}
