package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

class Query20(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q20"

  override def execute() = {
    import spark.implicits._
    val part = loadTable("part", new PartParser).toDS()
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val partsupp = loadTable("partsupp", new PartsuppParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()

    part.createOrReplaceTempView("part")
    partsupp.createOrReplaceTempView("partsupp")
    supplier.createOrReplaceTempView("supplier")
    nation.createOrReplaceTempView("nation")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """
        |select
        |	s_name,
        |	s_address
        |from
        |	supplier,
        |	nation
        |where
        |	s_suppkey in (
        |		select
        |			ps_suppkey
        |		from
        |			partsupp
        |		where
        |			ps_partkey in (
        |				select
        |					p_partkey
        |				from
        |					part
        |				where
        |					p_name like 'forest%'
        |			)
        |			and ps_availqty > (
        |				select
        |					0.5 * sum(l_quantity)
        |				from
        |					lineitem
        |				where
        |					l_partkey = ps_partkey
        |					and l_suppkey = ps_suppkey
        |					and l_shipdate >= date '1994-01-01'
        |					and l_shipdate < date '1994-01-01' + interval '1' year
        |			)
        |	)
        |	and s_nationkey = n_nationkey
        |	and n_name = 'CANADA'
        |order by
        |	s_name
      """.stripMargin)
  }
}
