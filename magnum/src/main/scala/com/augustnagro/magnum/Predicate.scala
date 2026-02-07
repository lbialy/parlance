package com.augustnagro.magnum

/** ADT for SQL predicates with correct parenthesization. */
enum Predicate:
  case Leaf(frag: Frag)
  case And(children: Vector[Predicate])
  case Or(children: Vector[Predicate])

  /** Render this predicate tree to a Frag with correct parenthesization. */
  def toFrag: Frag = this match
    case Leaf(frag) => frag

    case And(children) =>
      val nonEmpty = children.map(_.toFrag).filter(_.sqlString.nonEmpty)
      nonEmpty.size match
        case 0 => Frag("", Seq.empty, FragWriter.empty)
        case 1 => nonEmpty.head
        case _ => Predicate.joinFrags(nonEmpty, " AND ")

    case Or(children) =>
      val nonEmpty = children.map(_.toFrag).filter(_.sqlString.nonEmpty)
      nonEmpty.size match
        case 0 => Frag("", Seq.empty, FragWriter.empty)
        case 1 => nonEmpty.head
        case _ => Predicate.joinFrags(nonEmpty, " OR ")

object Predicate:
  val empty: Predicate = And(Vector.empty)

  private def joinFrags(frags: Vector[Frag], separator: String): Frag =
    val sb = new StringBuilder("(")
    val allParams = Vector.newBuilder[Any]
    val writers = Vector.newBuilder[FragWriter]

    var first = true
    for f <- frags do
      if !first then sb.append(separator)
      first = false
      sb.append(f.sqlString)
      allParams ++= f.params
      writers += f.writer

    sb.append(")")

    val combinedWriter: FragWriter = (ps, pos) =>
      var currentPos = pos
      for w <- writers.result() do currentPos = w.write(ps, currentPos)
      currentPos

    Frag(sb.result(), allParams.result(), combinedWriter)
