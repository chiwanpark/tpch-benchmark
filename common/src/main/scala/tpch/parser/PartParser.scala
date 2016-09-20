package tpch.parser

import tpch.Schema.Part

class PartParser extends Parser[Part]{
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(partKey, name, mfgr, brand, tpe, size, container, retailPrice, comment) =>
        Some(Part(partKey.toInt, name, mfgr, brand, tpe, size.toInt, container, retailPrice.toDouble, comment))
      case _ => None
    }
  }
}
