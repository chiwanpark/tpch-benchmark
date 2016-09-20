package tpch.parser

import tpch.Schema.Order

class OrderParser extends Parser[Order] {
  override def parse(line: String) = {
    preprocess(line) match {
      case Array(orderKey, custKey, orderStatus, totalPrice, orderDate, orderPriority, clerk, shipPriority, comment) =>
        Some(Order(orderKey.toInt, custKey.toInt, orderStatus, totalPrice.toDouble, orderDate, orderPriority, clerk, shipPriority.toInt, comment))
      case _ => None
    }
  }
}
