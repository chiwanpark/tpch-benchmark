package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

class Query11(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q11"

  override def execute() = {
    import spark.implicits._
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val partsupp = loadTable("partsupp", new PartsuppParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()

    supplier.createOrReplaceTempView("supplier")
    partsupp.createOrReplaceTempView("partsupp")
    nation.createOrReplaceTempView("nation")

    spark.sql(
      """select
        |	ps_partkey,
        |	sum(ps_supplycost * ps_availqty) as value
        |from
        |	partsupp,
        |	supplier,
        |	nation
        |where
        |	ps_suppkey = s_suppkey
        |	and s_nationkey = n_nationkey
        |	and n_name = 'GERMANY'
        |group by
        |	ps_partkey having
        |		sum(ps_supplycost * ps_availqty) > (
        |			select
        |				sum(ps_supplycost * ps_availqty) * 0.0001000000
        |			from
        |				partsupp,
        |				supplier,
        |				nation
        |			where
        |				ps_suppkey = s_suppkey
        |				and s_nationkey = n_nationkey
        |				and n_name = 'GERMANY'
        |		)
        |order by
        |	value desc
      """.stripMargin)
  }
}
