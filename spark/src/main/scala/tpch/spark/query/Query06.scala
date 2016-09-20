package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser.LineitemParser

class Query06(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q06"

  override def execute() = {
    import spark.implicits._
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()

    lineitem.createOrReplaceTempView("lineitem")

    spark.sql(
      """select
        |	sum(l_extendedprice * l_discount) as revenue
        |from
        |	lineitem
        |where
        |	l_shipdate >= date '1994-01-01'
        |	and l_shipdate < date '1994-01-01' + interval '1' year
        |	and l_discount between .06 - 0.01 and .06 + 0.01
        |	and l_quantity < 24
      """.stripMargin)
  }
}
