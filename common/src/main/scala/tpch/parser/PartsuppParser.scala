package tpch.parser

import tpch.Schema.Partsupp

class PartsuppParser extends Parser[Partsupp] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(partKey, suppKey, availQty, supplyCost, comment) =>
        Some(Partsupp(partKey.toInt, suppKey.toInt, availQty.toInt, supplyCost.toDouble, comment))
      case _ => None
    }
  }
}
