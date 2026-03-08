package ma.chinespirit.parlance.migrate

class MigrationError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
