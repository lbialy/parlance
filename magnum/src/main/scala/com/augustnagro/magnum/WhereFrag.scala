package com.augustnagro.magnum

object WhereFrag:
  opaque type WhereFrag <: Frag = Frag
  private[magnum] inline def apply(f: Frag): WhereFrag = f
  val empty: WhereFrag = Frag("", Seq.empty, FragWriter.empty)
  extension (f: Frag)
    def unsafeAsWhere: WhereFrag = f

export WhereFrag.WhereFrag
export WhereFrag.unsafeAsWhere
