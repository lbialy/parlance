package com.augustnagro.magnum

import java.sql.PreparedStatement

trait FragWriter:
  /** Writes a Frag's values to `ps`, staring at postion `pos`. Returns the new position.
    */
  def write(ps: PreparedStatement, pos: Int): Int

object FragWriter:
  val empty: FragWriter = (_, pos) => pos

  def fromKeys(keys: Vector[Any]): FragWriter = (ps, pos) =>
    var i = pos
    keys.foreach: key =>
      ps.setObject(i, key)
      i += 1
    i

  /** Write a single JDBC array parameter using Connection.createArrayOf. */
  def anyArray(keys: Vector[Any], jdbcTypeName: String): FragWriter = (ps, pos) =>
    val arr = ps.getConnection.createArrayOf(jdbcTypeName, keys.toArray[Any].asInstanceOf[Array[AnyRef]])
    ps.setArray(pos, arr)
    pos + 1
