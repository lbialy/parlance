package ma.chinespirit.parlance

class QueryBuilderException(message: String) extends RuntimeException(message)

object QueryBuilderException:
  def requireProduct(entity: Any, tableName: String): Product =
    entity match
      case p: Product => p
      case _ =>
        throw QueryBuilderException(
          s"Entity of type ${entity.getClass.getName} for table '$tableName' is not a Product (case class)"
        )

  def requireNonEmpty[A](results: Vector[A], context: String): A =
    results.headOption.getOrElse(
      throw QueryBuilderException(s"$context returned no results")
    )
