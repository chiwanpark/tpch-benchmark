package tpch.spark.query

import org.apache.spark.sql.functions._
import tpch.conf.Configuration
import tpch.parser._

class Query15(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q15"

  override def execute() = {
    import spark.implicits._
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val supplier = loadTable("supplier", new SupplierParser).toDS()

    supplier.createOrReplaceTempView("supplier")
    lineitem.createOrReplaceTempView("lineitem")

    // step 1: revenue0
    // define unsupported UDF
    val discount = udf((original: Double, discount: Double) => original * (1 - discount))

    // pre-process
    val cdLineitem = lineitem.filter($"l_shipdate" >= "1996-01-01" && $"l_shipdate" < "1996-04-01") // custom impl for interval
    val revenue0 = cdLineitem.select($"l_suppkey" as "supplier_no", discount($"l_extendedprice", $"l_discount") as "revenue")
      .groupBy($"supplier_no")
      .agg(sum($"revenue") as "total_revenue")

    revenue0.createOrReplaceTempView("revenue0")

    // step 2: remaining query
    spark.sql(
      """
        |select
        |	s_suppkey,
        |	s_name,
        |	s_address,
        |	s_phone,
        |	total_revenue
        |from
        |	supplier,
        |	revenue0
        |where
        |	s_suppkey = supplier_no
        |	and total_revenue = (
        |		select
        |			max(total_revenue)
        |		from
        |			revenue0
        |	)
        |order by
        |	s_suppkey
      """.stripMargin
    )
  }
}
