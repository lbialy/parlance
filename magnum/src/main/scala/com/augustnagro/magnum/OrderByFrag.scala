package com.augustnagro.magnum

object OrderByFrag:
  opaque type OrderByFrag <: Frag = Frag
  private[magnum] inline def apply(f: Frag): OrderByFrag = f
  val empty: OrderByFrag = Frag("", Seq.empty, FragWriter.empty)
  extension (f: Frag)
    def unsafeAsOrderBy: OrderByFrag = f

export OrderByFrag.OrderByFrag
export OrderByFrag.unsafeAsOrderBy
