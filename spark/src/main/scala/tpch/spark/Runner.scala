package tpch.spark

import tpch.conf.{Configuration, ConfigurationParser}
import tpch.spark.query._

object Runner {
  def main(args: Array[String]): Unit = {
    val safeArgs = if (args == null) Array.empty else args
    val conf = (new ConfigurationParser).parse(safeArgs, Configuration()) match {
      case Some(c) => c
      case None => sys.exit(-1)
    }

    val result = conf.numQuery match {
      case 1 => new Query01(conf).execute()
      case 2 => new Query02(conf).execute()
      case 3 => new Query03(conf).execute()
      case 4 => new Query04(conf).execute()
      case 5 => new Query05(conf).execute()
      case 6 => new Query06(conf).execute()
      case 7 => new Query07(conf).execute()
      case 8 => new Query08(conf).execute()
      case 9 => new Query09(conf).execute()
      case 10 => new Query10(conf).execute()
      case 11 => new Query11(conf).execute()
      case 12 => new Query12(conf).execute()
      case 13 => new Query13(conf).execute()
      case 14 => new Query14(conf).execute()
      case 15 => new Query15(conf).execute()
      case 16 => new Query16(conf).execute()
      case 17 => new Query17(conf).execute()
      case 18 => new Query18(conf).execute()
      case 19 => new Query19(conf).execute()
      case 20 => new Query20(conf).execute()
      case 21 => new Query21(conf).execute()
      case 22 => new Query22(conf).execute()
    }

    result.show()
  }
}
