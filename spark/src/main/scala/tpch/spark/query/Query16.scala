package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

class Query16(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q16"

  override def execute() = {
    import spark.implicits._
    val part = loadTable("part", new PartParser).toDS()
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val partsupp = loadTable("partsupp", new PartsuppParser).toDS()

    part.createOrReplaceTempView("part")
    supplier.createOrReplaceTempView("supplier")
    partsupp.createOrReplaceTempView("partsupp")

    spark.sql(
      """
        |select
        |	p_brand,
        |	p_type,
        |	p_size,
        |	count(distinct ps_suppkey) as supplier_cnt
        |from
        |	partsupp,
        |	part
        |where
        |	p_partkey = ps_partkey
        |	and p_brand <> 'Brand#45'
        |	and p_type not like 'MEDIUM POLISHED%'
        |	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
        |	and ps_suppkey not in (
        |		select
        |			s_suppkey
        |		from
        |			supplier
        |		where
        |			s_comment like '%Customer%Complaints%'
        |	)
        |group by
        |	p_brand,
        |	p_type,
        |	p_size
        |order by
        |	supplier_cnt desc,
        |	p_brand,
        |	p_type,
        |	p_size
      """.stripMargin)
  }
}
