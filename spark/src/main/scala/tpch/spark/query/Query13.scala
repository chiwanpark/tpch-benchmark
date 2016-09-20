package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser.{CustomerParser, OrderParser}

import org.apache.spark.sql.functions._

class Query13(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q13"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val customer = loadTable("customer", new CustomerParser).toDS()

    // define an UDF for not like operation with Scalaesque API
    val notLike = udf((line: String) => !line.matches(".*special.*requests.*"))

    // pre-process (filter and select)
    val cdOrders = orders.filter(notLike($"o_comment")).select($"o_custkey", $"o_orderkey")
    val cdCustomer = customer.select($"c_custkey")

    // c_orders (group by + count after left-outer join)
    val cOrders = cdCustomer.join(cdOrders, $"c_custkey" === cdOrders("o_custkey"), "left_outer")
      .select($"c_custkey", $"o_orderkey")
      .groupBy($"c_custkey")
      .agg(count($"o_orderkey") as "c_count")

    // group by + aggregation + order by
    cOrders.groupBy($"c_count")
      .agg(count("c_custkey") as "custdist") // we can use c_custkey instead of * because there are only two columns.
      .sort($"custdist".desc, $"c_count".desc)
  }
}
