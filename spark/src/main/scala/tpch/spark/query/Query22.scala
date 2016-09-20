package tpch.spark.query

import org.apache.spark.sql.functions._
import tpch.conf.Configuration
import tpch.parser.{CustomerParser, OrderParser}

class Query22(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q22"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val customer = loadTable("customer", new CustomerParser).toDS()

    orders.createOrReplaceTempView("orders")
    customer.createOrReplaceTempView("customer")

    val cdSubstrings = Array("13", "31", "23", "29", "30", "18", "17")
    val substring = udf((line: String) => line.substring(0, 2)) // substring in SQL Server starts at 1
    val candidateCheck = udf((line: String) => cdSubstrings.contains(line))

    // pre-process
    val cdCustomer = customer.select($"c_custkey", substring($"c_phone") as "cntrycode", $"c_acctbal")
    val cdOrders = orders.select($"o_custkey")

    // calculate average, eager execution (is there any better method?)
    val avgBalance = cdCustomer.filter(candidateCheck($"cntrycode") && $"c_acctbal" > 0)
      .agg(avg($"c_acctbal") as "avg_acctbal").collect().head.getAs[Double]("avg_acctbal")

    // custsale
    val custSale = cdCustomer.filter(candidateCheck($"cntrycode") && $"c_acctbal" > avgBalance)
      .join(cdOrders, $"c_custkey" === cdOrders("o_custkey"), "left_outer")
      .filter($"o_custkey".isNull) // left-outer join with null-check filter instead of exists

    // group by + aggregation + order by
    custSale.groupBy($"cntrycode")
      .agg(count($"c_acctbal") as "numcust", sum($"c_acctbal") as "totacctbal")
      .sort($"cntrycode")
  }
}
