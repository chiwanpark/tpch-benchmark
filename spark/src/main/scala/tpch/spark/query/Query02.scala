package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

class Query02(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q02"

  override def execute() = {
    import spark.implicits._
    val part = loadTable("part", new PartParser).toDS()
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val partsupp = loadTable("partsupp", new PartsuppParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()
    val region = loadTable("region", new RegionParser).toDS()

    part.createOrReplaceTempView("part")
    supplier.createOrReplaceTempView("supplier")
    partsupp.createOrReplaceTempView("partsupp")
    nation.createOrReplaceTempView("nation")
    region.createOrReplaceTempView("region")

    spark.sql(
      """select
        |	s_acctbal,
        |	s_name,
        |	n_name,
        |	p_partkey,
        |	p_mfgr,
        |	s_address,
        |	s_phone,
        |	s_comment
        |from
        |	part,
        |	supplier,
        |	partsupp,
        |	nation,
        |	region
        |where
        |	p_partkey = ps_partkey
        |	and s_suppkey = ps_suppkey
        |	and p_size = 15
        |	and p_type like '%BRASS'
        |	and s_nationkey = n_nationkey
        |	and n_regionkey = r_regionkey
        |	and r_name = 'EUROPE'
        |	and ps_supplycost = (
        |		select
        |			min(ps_supplycost)
        |		from
        |			partsupp,
        |			supplier,
        |			nation,
        |			region
        |		where
        |			p_partkey = ps_partkey
        |			and s_suppkey = ps_suppkey
        |			and s_nationkey = n_nationkey
        |			and n_regionkey = r_regionkey
        |			and r_name = 'EUROPE'
        |	)
        |order by
        |	s_acctbal desc,
        |	n_name,
        |	s_name,
        |	p_partkey
      """.stripMargin)
  }
}
