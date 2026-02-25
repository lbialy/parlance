package com.augustnagro.magnum

class SelectExpr[A](
    override val queryRepr: String,
    val alias: String,
    val codec: DbCodec[A]
) extends ColRef[A]:
  def scalaName: String = alias
  def sqlName: String = alias
