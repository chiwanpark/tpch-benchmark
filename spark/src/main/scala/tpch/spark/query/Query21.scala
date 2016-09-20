package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

class Query21(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q21"

  override def execute() = {
    import spark.implicits._
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val orders = loadTable("orders", new OrderParser).toDS()

    orders.createOrReplaceTempView("orders")
    supplier.createOrReplaceTempView("supplier")
    nation.createOrReplaceTempView("nation")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """
        |select
        |	s_name,
        |	count(*) as numwait
        |from
        |	supplier,
        |	lineitem l1,
        |	orders,
        |	nation
        |where
        |	s_suppkey = l1.l_suppkey
        |	and o_orderkey = l1.l_orderkey
        |	and o_orderstatus = 'F'
        |	and l1.l_receiptdate > l1.l_commitdate
        |	and exists (
        |		select
        |			*
        |		from
        |			lineitem l2
        |		where
        |			l2.l_orderkey = l1.l_orderkey
        |			and l2.l_suppkey <> l1.l_suppkey
        |	)
        |	and not exists (
        |		select
        |			*
        |		from
        |			lineitem l3
        |		where
        |			l3.l_orderkey = l1.l_orderkey
        |			and l3.l_suppkey <> l1.l_suppkey
        |			and l3.l_receiptdate > l3.l_commitdate
        |	)
        |	and s_nationkey = n_nationkey
        |	and n_name = 'SAUDI ARABIA'
        |group by
        |	s_name
        |order by
        |	numwait desc,
        |	s_name
      """.stripMargin)
  }
}
