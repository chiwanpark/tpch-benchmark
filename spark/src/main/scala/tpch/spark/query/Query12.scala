package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser.{LineitemParser, OrderParser}

class Query12(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q12"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()

    orders.createOrReplaceTempView("orders")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """select
        |	l_shipmode,
        |	sum(case
        |		when o_orderpriority = '1-URGENT'
        |			or o_orderpriority = '2-HIGH'
        |			then 1
        |		else 0
        |	end) as high_line_count,
        |	sum(case
        |		when o_orderpriority <> '1-URGENT'
        |			and o_orderpriority <> '2-HIGH'
        |			then 1
        |		else 0
        |	end) as low_line_count
        |from
        |	orders,
        |	lineitem
        |where
        |	o_orderkey = l_orderkey
        |	and l_shipmode in ('MAIL', 'SHIP')
        |	and l_commitdate < l_receiptdate
        |	and l_shipdate < l_commitdate
        |	and l_receiptdate >= date '1994-01-01'
        |	and l_receiptdate < date '1994-01-01' + interval '1' year
        |group by
        |	l_shipmode
        |order by
        |	l_shipmode
      """.stripMargin)
  }
}
