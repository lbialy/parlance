package com.augustnagro.magnum

import java.sql.PreparedStatement

extension [A](col: ColRef[A])

  def ===(value: A)(using codec: DbCodec[A]): Frag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    Frag(s"${col.queryRepr} = ?", Seq(value), writer)

  def !==(value: A)(using codec: DbCodec[A]): Frag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    Frag(s"${col.queryRepr} <> ?", Seq(value), writer)

  def >(value: A)(using codec: DbCodec[A]): Frag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    Frag(s"${col.queryRepr} > ?", Seq(value), writer)

  def <(value: A)(using codec: DbCodec[A]): Frag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    Frag(s"${col.queryRepr} < ?", Seq(value), writer)

  def >=(value: A)(using codec: DbCodec[A]): Frag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    Frag(s"${col.queryRepr} >= ?", Seq(value), writer)

  def <=(value: A)(using codec: DbCodec[A]): Frag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(value, ps, pos)
      pos + codec.cols.length
    Frag(s"${col.queryRepr} <= ?", Seq(value), writer)

  def isNull: Frag =
    Frag(s"${col.queryRepr} IS NULL", Seq.empty, FragWriter.empty)

  def isNotNull: Frag =
    Frag(s"${col.queryRepr} IS NOT NULL", Seq.empty, FragWriter.empty)

  def in(values: Iterable[A])(using codec: DbCodec[A]): Frag =
    val vals = values.toVector
    if vals.isEmpty then Frag("1 = 0", Seq.empty, FragWriter.empty)
    else
      val placeholders = vals.map(_ => "?").mkString(", ")
      val writer: FragWriter = (ps, pos) =>
        var currentPos = pos
        for v <- vals do
          codec.writeSingle(v, ps, currentPos)
          currentPos += codec.cols.length
        currentPos
      Frag(s"${col.queryRepr} IN ($placeholders)", vals, writer)

extension (col: ColRef[String])
  def like(pattern: String)(using codec: DbCodec[String]): Frag =
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(pattern, ps, pos)
      pos + codec.cols.length
    Frag(s"${col.queryRepr} LIKE ?", Seq(pattern), writer)
