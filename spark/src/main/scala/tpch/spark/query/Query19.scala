package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser.{LineitemParser, PartParser}

class Query19(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q19"

  override def execute() = {
    import spark.implicits._
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val part = loadTable("part", new PartParser).toDS()

    lineitem.createOrReplaceTempView("lineitem")
    part.createOrReplaceTempView("part")

    spark.sql(
      """
        |select
        |	sum(l_extendedprice* (1 - l_discount)) as revenue
        |from
        |	lineitem,
        |	part
        |where
        |	(
        |		p_partkey = l_partkey
        |		and p_brand = 'Brand#12'
        |		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        |		and l_quantity >= 1 and l_quantity <= 1 + 10
        |		and p_size between 1 and 5
        |		and l_shipmode in ('AIR', 'AIR REG')
        |		and l_shipinstruct = 'DELIVER IN PERSON'
        |	)
        |	or
        |	(
        |		p_partkey = l_partkey
        |		and p_brand = 'Brand#23'
        |		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        |		and l_quantity >= 10 and l_quantity <= 10 + 10
        |		and p_size between 1 and 10
        |		and l_shipmode in ('AIR', 'AIR REG')
        |		and l_shipinstruct = 'DELIVER IN PERSON'
        |	)
        |	or
        |	(
        |		p_partkey = l_partkey
        |		and p_brand = 'Brand#34'
        |		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        |		and l_quantity >= 20 and l_quantity <= 20 + 10
        |		and p_size between 1 and 15
        |		and l_shipmode in ('AIR', 'AIR REG')
        |		and l_shipinstruct = 'DELIVER IN PERSON'
        |	)
      """.stripMargin)
  }
}
