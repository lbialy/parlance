package com.augustnagro.magnum.migrate

case class VerifyResult(
    table: String,
    entityName: String,
    issues: List[VerifyIssue],
    columnDetails: List[VerifyColumnDetail] = Nil
):
  def isOk: Boolean = issues.forall(_.isWarning)
  def hasErrors: Boolean = issues.exists(!_.isWarning)

  def prettyPrint: String =
    val sb = StringBuilder()
    sb.append(s"verify[$entityName] (\"$table\")\n")

    for detail <- columnDetails do
      val status = detail.issue match
        case None    => "\u2713"
        case Some(i) => if i.isWarning then "\u26a0" else "\u2717"
      val scalaType = detail.scalaType.padTo(16, ' ')
      val dbInfo = detail.dbInfo
      val suffix = detail.issue match
        case None    => ""
        case Some(i) => s" \u2014 ${issueLabel(i)}"
      sb.append(s"  $status ${detail.columnName.padTo(18, ' ')} $scalaType \u2194  $dbInfo$suffix\n")

    for issue <- issues do
      issue match
        case VerifyIssue.TableMissing(tn) =>
          sb.append(s"  \u2717 table \"$tn\" does not exist\n")
        case VerifyIssue.ExtraColumnInDb(col) =>
          sb.append(s"  \u26a0 $col${" " * (18 - col.length).max(0)} \u2014               \u2014 extra column in DB\n")
        case VerifyIssue.PrimaryKeyMismatch(expected, actual) =>
          sb.append(
            s"  \u2717 primary key mismatch: expected [${expected.mkString(", ")}], actual [${actual.mkString(", ")}]\n"
          )
        case _ => () // already rendered via columnDetails

    sb.result()
  end prettyPrint

  private def issueLabel(i: VerifyIssue): String = i match
    case _: VerifyIssue.TypeMismatch        => "TypeMismatch"
    case _: VerifyIssue.NullabilityMismatch => "NullabilityMismatch"
    case _: VerifyIssue.ColumnMissing       => "ColumnMissing"
    case other                              => other.productPrefix

end VerifyResult

case class VerifyColumnDetail(
    columnName: String,
    scalaType: String,
    dbInfo: String,
    issue: Option[VerifyIssue]
)

enum VerifyIssue(val isWarning: Boolean):
  case TableMissing(tableName: String) extends VerifyIssue(false)
  case ColumnMissing(columnName: String, scalaType: String) extends VerifyIssue(false)
  case TypeMismatch(
      columnName: String,
      scalaType: String,
      dbType: String
  ) extends VerifyIssue(false)
  case NullabilityMismatch(
      columnName: String,
      isOptionInScala: Boolean,
      isNullableInDb: Boolean
  ) extends VerifyIssue(true)
  case ExtraColumnInDb(columnName: String) extends VerifyIssue(true)
  case PrimaryKeyMismatch(expected: List[String], actual: List[String]) extends VerifyIssue(false)
