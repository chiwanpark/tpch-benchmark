package tpch.spark.query

import tpch.conf.Configuration
import tpch.parser._

import org.apache.spark.sql.functions.udf

class Query07(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q07"

  override def execute() = {
    import spark.implicits._
    val orders = loadTable("orders", new OrderParser).toDS()
    val customer = loadTable("customer", new CustomerParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()

    // define unsupported UDF
    val extract = udf((year: String) => year.substring(0, 4))
    val discount = udf((original: Double, discount: Double) => original * (1 - discount))

    // pre-process (filter and select) to reduce intermediate data
    val cdNation = nation.filter($"n_name" === "FRANCE" || $"n_name" === "GERMANY")
    val cdLineitem = lineitem.filter($"l_shipdate" >= "1995-01-01" && $"l_shipdate" <= "1996-12-31")
      .select($"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate", $"l_suppkey")
    val cdOrders = orders.select($"o_orderkey", $"o_custkey")

    // join nation with supplier (n1)
    val supplierSide = cdNation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"n_name" as "supp_nation", $"s_suppkey")
      .join(cdLineitem, $"s_suppkey" === cdLineitem("l_suppkey"))

    // join nation with customer (n2)
    val customerSide = cdNation.join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"n_name" as "cust_nation", $"c_custkey")
      .join(cdOrders, $"c_custkey" === cdOrders("o_custkey"))

    // main join operation in subquery
    val shipping = supplierSide.join(customerSide, $"l_orderkey" === customerSide("o_orderkey"))
      .filter(($"supp_nation" === "FRANCE" && $"cust_nation" === "GERMANY") ||
        ($"supp_nation" === "GERMANY" && $"cust_nation" === "FRANCE"))
      .select($"supp_nation", $"cust_nation", extract($"l_shipdate") as "l_year",
        discount($"l_extendedprice", $"l_discount") as "volume")

    shipping.createOrReplaceTempView("shipping")

    // remaining query
    spark.sql(
      """select
        |	supp_nation,
        |	cust_nation,
        |	l_year,
        |	sum(volume) as revenue
        |from shipping
        |group by
        |	supp_nation,
        |	cust_nation,
        |	l_year
        |order by
        |	supp_nation,
        |	cust_nation,
        |	l_year
      """.stripMargin)
  }
}
