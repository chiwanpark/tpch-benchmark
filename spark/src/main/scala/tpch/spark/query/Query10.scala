package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

class Query10(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q10"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val customer = loadTable("customer", new CustomerParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()

    nation.createOrReplaceTempView("nation")
    orders.createOrReplaceTempView("orders")
    customer.createOrReplaceTempView("customer")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """select
        |	c_custkey,
        |	c_name,
        |	sum(l_extendedprice * (1 - l_discount)) as revenue,
        |	c_acctbal,
        |	n_name,
        |	c_address,
        |	c_phone,
        |	c_comment
        |from
        |	customer,
        |	orders,
        |	lineitem,
        |	nation
        |where
        |	c_custkey = o_custkey
        |	and l_orderkey = o_orderkey
        |	and o_orderdate >= date '1993-10-01'
        |	and o_orderdate < date '1993-10-01' + interval '3' month
        |	and l_returnflag = 'R'
        |	and c_nationkey = n_nationkey
        |group by
        |	c_custkey,
        |	c_name,
        |	c_acctbal,
        |	c_phone,
        |	n_name,
        |	c_address,
        |	c_comment
        |order by
        |	revenue desc
      """.stripMargin)
  }
}
