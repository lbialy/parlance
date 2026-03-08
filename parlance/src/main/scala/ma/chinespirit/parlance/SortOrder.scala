package ma.chinespirit.parlance

enum SortOrder extends SqlLiteral:
  case Default, Asc, Desc
  def queryRepr: String = this match
    case Default => ""
    case Asc     => "ASC"
    case Desc    => "DESC"
