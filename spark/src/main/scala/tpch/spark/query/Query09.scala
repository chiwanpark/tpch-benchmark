package tpch.spark.query

import org.apache.spark.sql.functions._
import tpch.conf.Configuration
import tpch.parser._

class Query09(override val configuration: Configuration) extends QueryExecution {
  override def queryName = "Q09"

  override def execute() = {
    import spark.implicits._
    val part = loadTable("part", new PartParser).toDS()
    val orders = loadTable("orders", new OrderParser).toDS()
    val lineitem = loadTable("lineitem", new LineitemParser).toDS()
    val supplier = loadTable("supplier", new SupplierParser).toDS()
    val nation = loadTable("nation", new NationParser).toDS()
    val partsupp = loadTable("partsupp", new PartsuppParser).toDS()

    // define unsupported UDF
    val extract = udf((year: String) => year.substring(0, 4))
    val amount = udf((original: Double, discount: Double, supplyCost: Double, quantity: Double) =>
      original * (1 - discount) - (supplyCost * quantity))

    // pre-process (filter and select)
    val cdPart = part.filter($"p_name" like "%green%").select($"p_partkey")
    val cdPartsupp = partsupp.select($"ps_suppkey", $"ps_partkey", $"ps_supplycost")
    val cdNation = nation.select($"n_name" as "nation", $"n_nationkey")
    val cdOrders = orders.select(extract($"o_orderdate") as "o_year", $"o_orderkey")
    val cdSupplier = supplier.select($"s_nationkey", $"s_suppkey")
    val cdLineitem = lineitem
      .select($"l_discount", $"l_quantity", $"l_extendedprice", $"l_suppkey", $"l_partkey", $"l_orderkey")

    // join supplier with nation
    val supplierSide = cdNation.join(cdSupplier, $"n_nationkey" === cdSupplier("s_nationkey"))
      .select($"nation", $"s_suppkey")

    // join lineitem with part, partsupp and orders
    val lineitemSide = cdPart.join(cdLineitem, $"p_partkey" === cdLineitem("l_partkey"))
      .select($"l_suppkey", $"l_partkey", $"l_orderkey", $"l_discount", $"l_quantity", $"l_extendedprice")
      .join(cdPartsupp, $"l_suppkey" === cdPartsupp("ps_suppkey") && $"l_partkey" === cdPartsupp("ps_partkey"))
      .select(amount($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity") as "amount",
        $"l_suppkey", $"l_orderkey")
      .join(cdOrders, $"l_orderkey" === cdOrders("o_orderkey"))
      .select($"l_suppkey", $"o_year", $"amount")

    // main join operation
    val profit = supplierSide.join(lineitemSide, $"s_suppkey" === lineitemSide("l_suppkey"))
      .select($"nation", $"o_year", $"amount")

    // group by and aggregation
    profit.groupBy($"nation", $"o_year").agg(sum($"amount") as "sum_profit").sort($"nation", $"o_year".desc)
  }
}
