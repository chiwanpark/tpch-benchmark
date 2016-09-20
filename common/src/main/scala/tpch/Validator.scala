package tpch

import tpch.conf.{ValidateConfiguration, ValidateConfigurationParser}
import tpch.parser.Parser
import tpch.parser.validation._

import scala.io.Source

object Validator {
  def validate[T <: Product](answer: Stream[String], result: Stream[String], parser: Parser[T]) = {
    val answerStream = answer.flatMap(parser.parse)
    val resultStream = result.flatMap(parser.parse)

    answerStream.zip(resultStream).foreach { case (ans, res) =>
      ans.productIterator.zip(res.productIterator).foreach {
        case (a: String, b: String) => require(a == b, s"Output is illegal! (ans: $a, res: $b)")
        case (a: Int, b: Int) => require(a == b, s"Output is illegal! (ans: $a, res: $b)")
        case (a: Double, b: Double) => require(math.abs(a - b) < 0.01, s"Output is illegal! (ans: $a, res: $b)")
        case (p, q) => throw new IllegalArgumentException(s"Output is illegal! (ans: $p, res: $q)")
      }
    }
  }

  def main(args: Array[String]) = {
    val safeArgs = if (args == null) Array.empty else args
    val conf = (new ValidateConfigurationParser).parse(safeArgs, ValidateConfiguration()) match {
      case Some(c) => c
      case None => sys.exit(-1)
    }

    val parser = conf.numQuery match {
      case 1 => new Query01Parser()
      case 2 => new Query02Parser()
      case 3 => new Query03Parser()
      case 4 => new Query04Parser()
      case 5 => new Query05Parser()
      case 6 => new Query06Parser()
      case 7 => new Query07Parser()
      case 8 => new Query08Parser()
      case 9 => new Query09Parser()
      case 10 => new Query10Parser()
      case 11 => new Query11Parser()
      case 12 => new Query12Parser()
      case 13 => new Query13Parser()
      case 14 => new Query14Parser()
      case 15 => new Query15Parser()
      case 16 => new Query16Parser()
      case 17 => new Query17Parser()
      case 18 => new Query18Parser()
      case 19 => new Query19Parser()
      case 20 => new Query20Parser()
      case 21 => new Query21Parser()
      case 22 => new Query22Parser()
    }

    println(s"Validation for TPC-H Query ${conf.numQuery}")
    validate(Source.fromFile(conf.answerPath).getLines().toStream,
      Source.fromFile(conf.resultPath).getLines().toStream, parser)
    println("Correct!")
  }
}
