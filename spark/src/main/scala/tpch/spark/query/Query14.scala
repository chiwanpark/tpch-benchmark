package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser.{LineitemParser, PartParser}

class Query14(override val configuration: Configuration) extends QueryExecution{
  override def queryName = "Q14"

  override def execute() = {
    import spark.implicits._
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val part = loadTable("part", new PartParser).toDS()

    lineitem.createOrReplaceTempView("lineitem")
    part.createOrReplaceTempView("part")

    spark.sql(
      """
        |select
        |	100.00 * sum(case
        |		when p_type like 'PROMO%'
        |			then l_extendedprice * (1 - l_discount)
        |		else 0
        |	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
        |from
        |	lineitem,
        |	part
        |where
        |	l_partkey = p_partkey
        |	and l_shipdate >= date '1995-09-01'
        |	and l_shipdate < date '1995-09-01' + interval '1' month
      """.stripMargin)
  }
}
