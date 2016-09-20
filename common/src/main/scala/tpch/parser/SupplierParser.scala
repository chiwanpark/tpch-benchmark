package tpch.parser

import tpch.Schema.Supplier

class SupplierParser extends Parser[Supplier] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(suppKey, name, address, nationKey, phone, acctBal, comment) =>
        Some(Supplier(suppKey.toInt, name, address, nationKey.toInt, phone, acctBal.toDouble, comment))
      case _ => None
    }
  }
}
