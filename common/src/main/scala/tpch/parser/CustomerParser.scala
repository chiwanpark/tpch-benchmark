package tpch.parser

import tpch.Schema.Customer

class CustomerParser extends Parser[Customer] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(custKey, name, address, nationKey, phone, acctBal, mktSegment, comment) =>
        Some(Customer(custKey.toInt, name, address, nationKey.toInt, phone, acctBal.toDouble, mktSegment, comment.trim))
      case _ => None
    }
  }
}
