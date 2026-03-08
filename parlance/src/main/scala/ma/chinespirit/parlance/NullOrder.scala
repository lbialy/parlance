package ma.chinespirit.parlance

enum NullOrder extends SqlLiteral:
  case Default, First, Last
  def queryRepr: String = this match
    case Default => ""
    case First   => "NULLS FIRST"
    case Last    => "NULLS LAST"
