package tpch.spark.query

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import tpch.conf.Configuration
import tpch.parser.Parser

import scala.reflect.ClassTag

trait QueryExecution {
  def queryName: String

  def configuration: Configuration

  protected lazy val spark = {
    SparkSession.builder()
      .appName(s"TPC-H $queryName")
      .config("spark.sql.crossJoin.enabled", value = true)
      .getOrCreate()
  }

  protected def loadTable[T: ClassTag](tableName: String, parser: Parser[T]): RDD[T] = {
    spark.sparkContext.textFile(s"${configuration.dataPath}/$tableName.tbl").flatMap(parser.parse)
  }

  def execute(): Dataset[Row]
}
