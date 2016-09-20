package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser.{CustomerParser, LineitemParser, OrderParser}

class Query03(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q03"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val customer = loadTable("customer", new CustomerParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()

    orders.createOrReplaceTempView("orders")
    customer.createOrReplaceTempView("customer")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """select
        |	l_orderkey,
        |	sum(l_extendedprice * (1 - l_discount)) as revenue,
        |	o_orderdate,
        |	o_shippriority
        |from
        |	customer,
        |	orders,
        |	lineitem
        |where
        |	c_mktsegment = 'BUILDING'
        |	and c_custkey = o_custkey
        |	and l_orderkey = o_orderkey
        |	and o_orderdate < date '1995-03-15'
        |	and l_shipdate > date '1995-03-15'
        |group by
        |	l_orderkey,
        |	o_orderdate,
        |	o_shippriority
        |order by
        |	revenue desc,
        |	o_orderdate
      """.stripMargin)
  }
}
