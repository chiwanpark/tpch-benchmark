package tpch.parser

import tpch.Schema.Region

class RegionParser extends Parser[Region] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(regionKey, name, comment) => Some(Region(regionKey.toInt, name, comment))
      case _ => None
    }
  }
}
