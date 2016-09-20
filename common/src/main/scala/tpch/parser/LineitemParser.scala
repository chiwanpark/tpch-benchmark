package tpch.parser

import tpch.Schema.Lineitem

class LineitemParser extends Parser[Lineitem] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(orderKey, partKey, suppKey, lineNumber, quantity, extendedPrice, discount, tax, returnFlag, lineStatus, shipDate, commitDate, receiptDate, shipInstruct, shipMode, comment) =>
        Some(Lineitem(orderKey.toInt, partKey.toInt, suppKey.toInt, lineNumber.toInt, quantity.toDouble, extendedPrice.toDouble, discount.toDouble, tax.toDouble, returnFlag, lineStatus, shipDate, commitDate, receiptDate, shipInstruct, shipMode, comment))
      case _ => None
    }
  }
}
