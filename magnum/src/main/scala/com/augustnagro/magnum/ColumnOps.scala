package com.augustnagro.magnum

import java.sql.PreparedStatement
import scala.annotation.targetName

extension [A](col: ColRef[A])

  def ===(value: A)(using codec: DbCodec[A]): WhereFrag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    WhereFrag(Frag(s"${col.queryRepr} = ?", Seq(value), writer))

  def !==(value: A)(using codec: DbCodec[A]): WhereFrag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    WhereFrag(Frag(s"${col.queryRepr} <> ?", Seq(value), writer))

  def >(value: A)(using codec: DbCodec[A]): WhereFrag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    WhereFrag(Frag(s"${col.queryRepr} > ?", Seq(value), writer))

  def <(value: A)(using codec: DbCodec[A]): WhereFrag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    WhereFrag(Frag(s"${col.queryRepr} < ?", Seq(value), writer))

  def >=(value: A)(using codec: DbCodec[A]): WhereFrag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    WhereFrag(Frag(s"${col.queryRepr} >= ?", Seq(value), writer))

  def <=(value: A)(using codec: DbCodec[A]): WhereFrag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    WhereFrag(Frag(s"${col.queryRepr} <= ?", Seq(value), writer))

  def isNull: WhereFrag =
    WhereFrag(Frag(s"${col.queryRepr} IS NULL", Seq.empty, FragWriter.empty))

  def isNotNull: WhereFrag =
    WhereFrag(Frag(s"${col.queryRepr} IS NOT NULL", Seq.empty, FragWriter.empty))

  def in(values: Iterable[A])(using codec: DbCodec[A]): WhereFrag =
    val vals = values.toVector
    if vals.isEmpty then WhereFrag(Frag("1 = 0", Seq.empty, FragWriter.empty))
    else
      val placeholders = vals.map(_ => "?").mkString(", ")
      val writer: FragWriter = (ps, pos) =>
        var currentPos = pos
        for v <- vals do
          codec.writeSingle(v, ps, currentPos)
          currentPos += codec.cols.length
        currentPos
      WhereFrag(Frag(s"${col.queryRepr} IN ($placeholders)", vals, writer))

  def notIn(values: Iterable[A])(using codec: DbCodec[A]): WhereFrag =
    val vals = values.toVector
    if vals.isEmpty then WhereFrag(Frag("1 = 1", Seq.empty, FragWriter.empty))
    else
      val placeholders = vals.map(_ => "?").mkString(", ")
      val writer: FragWriter = (ps, pos) =>
        var currentPos = pos
        for v <- vals do
          codec.writeSingle(v, ps, currentPos)
          currentPos += codec.cols.length
        currentPos
      WhereFrag(Frag(s"${col.queryRepr} NOT IN ($placeholders)", vals, writer))

  def between(low: A, high: A)(using codec: DbCodec[A]): WhereFrag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(low, ps, pos)
      val nextPos = pos + codec.cols.length
      codec.writeSingle(high, ps, nextPos)
      nextPos + codec.cols.length
    WhereFrag(Frag(s"${col.queryRepr} BETWEEN ? AND ?", Seq(low, high), writer))

  // Column-to-column comparison operators
  @targetName("eqCol")
  def ===(other: ColRef[A]): WhereFrag =
    WhereFrag(Frag(s"${col.queryRepr} = ${other.queryRepr}", Seq.empty, FragWriter.empty))

  @targetName("neCol")
  def !==(other: ColRef[A]): WhereFrag =
    WhereFrag(Frag(
      s"${col.queryRepr} <> ${other.queryRepr}",
      Seq.empty,
      FragWriter.empty
    ))

  @targetName("gtCol")
  def >(other: ColRef[A]): WhereFrag =
    WhereFrag(Frag(s"${col.queryRepr} > ${other.queryRepr}", Seq.empty, FragWriter.empty))

  @targetName("ltCol")
  def <(other: ColRef[A]): WhereFrag =
    WhereFrag(Frag(s"${col.queryRepr} < ${other.queryRepr}", Seq.empty, FragWriter.empty))

  @targetName("geCol")
  def >=(other: ColRef[A]): WhereFrag =
    WhereFrag(Frag(
      s"${col.queryRepr} >= ${other.queryRepr}",
      Seq.empty,
      FragWriter.empty
    ))

  @targetName("leCol")
  def <=(other: ColRef[A]): WhereFrag =
    WhereFrag(Frag(
      s"${col.queryRepr} <= ${other.queryRepr}",
      Seq.empty,
      FragWriter.empty
    ))
end extension

private def likeImpl(col: ColRef[?], op: String, pattern: String)(
    using codec: DbCodec[String]
): WhereFrag =
  val writer: FragWriter = (ps, pos) =>
    codec.writeSingle(pattern, ps, pos)
    pos + codec.cols.length
  WhereFrag(Frag(s"${col.queryRepr} $op ?", Seq(pattern), writer))

extension (col: ColRef[String])
  infix def like(pattern: String)(using DbCodec[String]): WhereFrag =
    likeImpl(col, "LIKE", pattern)

  infix def notLike(pattern: String)(using DbCodec[String]): WhereFrag =
    likeImpl(col, "NOT LIKE", pattern)

  infix def ilike(pattern: String)(using DbCodec[String], DbCon[? <: SupportsILike]): WhereFrag =
    likeImpl(col, "ILIKE", pattern)

extension (col: ColRef[Option[String]])
  @targetName("likeOpt")
  infix def like(pattern: String)(using DbCodec[String]): WhereFrag =
    likeImpl(col, "LIKE", pattern)

  @targetName("notLikeOpt")
  infix def notLike(pattern: String)(using DbCodec[String]): WhereFrag =
    likeImpl(col, "NOT LIKE", pattern)

  @targetName("ilikeOpt")
  infix def ilike(pattern: String)(using DbCodec[String], DbCon[? <: SupportsILike]): WhereFrag =
    likeImpl(col, "ILIKE", pattern)
