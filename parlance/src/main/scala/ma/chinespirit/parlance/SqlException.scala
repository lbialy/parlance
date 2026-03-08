package ma.chinespirit.parlance

class SqlException private[parlance] (message: String, cause: Throwable = null) extends RuntimeException(message, cause)
