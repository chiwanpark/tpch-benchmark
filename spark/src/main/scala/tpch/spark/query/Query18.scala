package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser.{CustomerParser, LineitemParser, OrderParser}

class Query18(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q18"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val customer = loadTable("customer", new CustomerParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()

    orders.createOrReplaceTempView("orders")
    customer.createOrReplaceTempView("customer")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """
        |select
        |	c_name,
        |	c_custkey,
        |	o_orderkey,
        |	o_orderdate,
        |	o_totalprice,
        |	sum(l_quantity)
        |from
        |	customer,
        |	orders,
        |	lineitem
        |where
        |	o_orderkey in (
        |		select
        |			l_orderkey
        |		from
        |			lineitem
        |		group by
        |			l_orderkey having
        |				sum(l_quantity) > 300
        |	)
        |	and c_custkey = o_custkey
        |	and o_orderkey = l_orderkey
        |group by
        |	c_name,
        |	c_custkey,
        |	o_orderkey,
        |	o_orderdate,
        |	o_totalprice
        |order by
        |	o_totalprice desc,
        |	o_orderdate
      """.stripMargin)
  }
}
