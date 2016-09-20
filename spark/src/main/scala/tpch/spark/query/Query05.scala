package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

class Query05(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q05"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val customer = loadTable("customer", new CustomerParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()
    val region = loadTable("region", new RegionParser).toDS()

    supplier.createOrReplaceTempView("supplier")
    nation.createOrReplaceTempView("nation")
    region.createOrReplaceTempView("region")
    orders.createOrReplaceTempView("orders")
    customer.createOrReplaceTempView("customer")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """select
        |	n_name,
        |	sum(l_extendedprice * (1 - l_discount)) as revenue
        |from
        |	customer,
        |	orders,
        |	lineitem,
        |	supplier,
        |	nation,
        |	region
        |where
        |	c_custkey = o_custkey
        |	and l_orderkey = o_orderkey
        |	and l_suppkey = s_suppkey
        |	and c_nationkey = s_nationkey
        |	and s_nationkey = n_nationkey
        |	and n_regionkey = r_regionkey
        |	and r_name = 'ASIA'
        |	and o_orderdate >= date '1994-01-01'
        |	and o_orderdate < date '1994-01-01' + interval '1' year
        |group by
        |	n_name
        |order by
        |	revenue desc
      """.stripMargin)
  }
}
