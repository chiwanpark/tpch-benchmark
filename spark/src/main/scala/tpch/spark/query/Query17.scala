package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

class Query17(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q17"

  override def execute() = {
    import spark.implicits._
    val part = loadTable("part", new PartParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()

    part.createOrReplaceTempView("part")
    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """
        |select
        |	sum(l_extendedprice) / 7.0 as avg_yearly
        |from
        |	lineitem,
        |	part
        |where
        |	p_partkey = l_partkey
        |	and p_brand = 'Brand#23'
        |	and p_container = 'MED BOX'
        |	and l_quantity < (
        |		select
        |			0.2 * avg(l_quantity)
        |		from
        |			lineitem
        |		where
        |			l_partkey = p_partkey
        |	)
      """.stripMargin)
  }
}
