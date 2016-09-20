package tpch.spark.query

import org.apache.spark.sql.functions._
import tpch.conf.Configuration
import tpch.parser._

class Query08(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q08"

  override def execute() = {
    import spark.implicits._
    val part = loadTable("part", new PartParser).toDS()
    val orders = loadTable("orders", new OrderParser).toDS()
    val customer = loadTable("customer", new CustomerParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()
    val region = loadTable("region", new RegionParser).toDS()

    // define unsupported UDF
    val extract = udf((year: String) => year.substring(0, 4))
    val discount = udf((original: Double, discount: Double) => original * (1 - discount))
    val brazilFilter = udf((nation: String, volume: Double) => if (nation == "BRAZIL") volume else 0)

    // pre-process (filter and select)
    val cdOrders = orders.filter($"o_orderdate" >= "1995-01-01" && $"o_orderdate" <= "1996-12-31")
      .select($"o_custkey", $"o_orderkey", $"o_orderdate")
    val cdRegion = region.filter($"r_name" === "AMERICA")
      .select($"r_regionkey")
    val cdPart = part.filter($"p_type" === "ECONOMY ANODIZED STEEL")
      .select($"p_partkey")
    val cdLineitem = lineitem.select($"l_partkey", $"l_suppkey", $"l_orderkey",
      discount($"l_extendedprice", $"l_discount") as "volume")
    val cdNation = nation.select($"n_name", $"n_regionkey", $"n_nationkey")
    val cdSupplier = supplier.select($"s_suppkey", $"s_nationkey")
    val cdCustomer = customer.select($"c_nationkey", $"c_custkey")

    // join lineitem with part
    val lineitemSide = cdPart.join(cdLineitem, $"p_partkey" === cdLineitem("l_partkey"))

    // join customer with nation and region
    val customerSide = cdNation.join(cdRegion, $"n_regionkey" === cdRegion("r_regionkey"))
      .join(cdCustomer, $"n_nationkey" === cdCustomer("c_nationkey"))
      .select($"c_custkey")
      .join(cdOrders, $"c_custkey" === cdOrders("o_custkey"))

    // join supplier with lineitem nad nation
    val supplierSide = cdNation.join(cdSupplier, $"n_nationkey" === cdSupplier("s_nationkey"))
      .join(lineitemSide, $"s_suppkey" === lineitemSide("l_suppkey"))

    // main join operation for all_nations
    val allNations = customerSide.join(supplierSide, $"o_orderkey" === supplierSide("l_orderkey"))
      .select(extract($"o_orderdate") as "o_year", $"volume", $"n_name" as "nation")

    // group by and aggregation
    allNations
      .select($"o_year", brazilFilter($"nation", $"volume") as "mkt_share", $"volume")
      .groupBy($"o_year")
      .agg(sum($"mkt_share") / sum("volume"))
      .sort($"o_year")
  }
}
