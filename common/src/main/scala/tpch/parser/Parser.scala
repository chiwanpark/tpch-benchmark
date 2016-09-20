package tpch.parser

trait Parser[T] extends Serializable {
  protected def preprocess(line: String) = line.split('|').map(_.trim)

  def parse(line: String): Option[T]
}
