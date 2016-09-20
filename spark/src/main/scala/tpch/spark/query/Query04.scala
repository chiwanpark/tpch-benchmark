package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser.{LineitemParser, OrderParser}

class Query04(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q04"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()

    orders.createOrReplaceTempView("orders")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """select
        |	o_orderpriority,
        |	count(*) as order_count
        |from
        |	orders
        |where
        |	o_orderdate >= date '1993-07-01'
        |	and o_orderdate < date '1993-07-01' + interval '3' month
        |	and exists (
        |		select
        |			*
        |		from
        |			lineitem
        |		where
        |			l_orderkey = o_orderkey
        |			and l_commitdate < l_receiptdate
        |	)
        |group by
        |	o_orderpriority
        |order by
        |	o_orderpriority
      """.stripMargin)
  }
}
