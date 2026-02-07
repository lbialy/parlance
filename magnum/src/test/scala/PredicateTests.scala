import com.augustnagro.magnum.*
import munit.FunSuite

class PredicateTests extends FunSuite:

  test("single leaf renders as-is"):
    val frag = Frag("x = 1", Seq.empty, FragWriter.empty)
    val pred = Predicate.Leaf(frag)
    val result = pred.toFrag
    assertEquals(result.sqlString, "x = 1")

  test("AND group renders with parentheses and AND separator"):
    val a = Predicate.Leaf(Frag("a = 1", Seq.empty, FragWriter.empty))
    val b = Predicate.Leaf(Frag("b = 2", Seq.empty, FragWriter.empty))
    val c = Predicate.Leaf(Frag("c = 3", Seq.empty, FragWriter.empty))
    val pred = Predicate.And(Vector(a, b, c))
    val result = pred.toFrag
    assertEquals(result.sqlString, "(a = 1 AND b = 2 AND c = 3)")

  test("OR group renders with parentheses and OR separator"):
    val a = Predicate.Leaf(Frag("a = 1", Seq.empty, FragWriter.empty))
    val b = Predicate.Leaf(Frag("b = 2", Seq.empty, FragWriter.empty))
    val c = Predicate.Leaf(Frag("c = 3", Seq.empty, FragWriter.empty))
    val pred = Predicate.Or(Vector(a, b, c))
    val result = pred.toFrag
    assertEquals(result.sqlString, "(a = 1 OR b = 2 OR c = 3)")

  test("nested: (a AND b) OR (c AND d) renders with correct parenthesization"):
    val a = Predicate.Leaf(Frag("a = 1", Seq.empty, FragWriter.empty))
    val b = Predicate.Leaf(Frag("b = 2", Seq.empty, FragWriter.empty))
    val c = Predicate.Leaf(Frag("c = 3", Seq.empty, FragWriter.empty))
    val d = Predicate.Leaf(Frag("d = 4", Seq.empty, FragWriter.empty))
    val andLeft = Predicate.And(Vector(a, b))
    val andRight = Predicate.And(Vector(c, d))
    val pred = Predicate.Or(Vector(andLeft, andRight))
    val result = pred.toFrag
    assertEquals(result.sqlString, "((a = 1 AND b = 2) OR (c = 3 AND d = 4))")

  test("empty AND is a no-op (empty sql)"):
    val pred = Predicate.And(Vector.empty)
    val result = pred.toFrag
    assertEquals(result.sqlString, "")

  test("empty OR is a no-op (empty sql)"):
    val pred = Predicate.Or(Vector.empty)
    val result = pred.toFrag
    assertEquals(result.sqlString, "")

  test("single-child AND renders without parentheses"):
    val a = Predicate.Leaf(Frag("x = 1", Seq.empty, FragWriter.empty))
    val pred = Predicate.And(Vector(a))
    val result = pred.toFrag
    assertEquals(result.sqlString, "x = 1")

  test("single-child OR renders without parentheses"):
    val a = Predicate.Leaf(Frag("x = 1", Seq.empty, FragWriter.empty))
    val pred = Predicate.Or(Vector(a))
    val result = pred.toFrag
    assertEquals(result.sqlString, "x = 1")

  test("params are preserved through AND"):
    val a = Predicate.Leaf(Frag("a = ?", Seq(1), FragWriter.empty))
    val b = Predicate.Leaf(Frag("b = ?", Seq(2), FragWriter.empty))
    val pred = Predicate.And(Vector(a, b))
    val result = pred.toFrag
    assertEquals(result.params.toList, List(1, 2))

  test("params are preserved through nested OR/AND"):
    val a = Predicate.Leaf(Frag("a = ?", Seq(1), FragWriter.empty))
    val b = Predicate.Leaf(Frag("b = ?", Seq("hello"), FragWriter.empty))
    val c = Predicate.Leaf(Frag("c = ?", Seq(3), FragWriter.empty))
    val d = Predicate.Leaf(Frag("d = ?", Seq(4), FragWriter.empty))
    val pred = Predicate.Or(Vector(
      Predicate.And(Vector(a, b)),
      Predicate.And(Vector(c, d))
    ))
    val result = pred.toFrag
    assertEquals(result.params.toList, List(1, "hello", 3, 4))

end PredicateTests
