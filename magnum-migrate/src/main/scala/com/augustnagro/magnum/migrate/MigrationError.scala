package com.augustnagro.magnum.migrate

class MigrationError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)
