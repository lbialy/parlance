package com.augustnagro.magnum.migrate

trait MigrationCompiler:
  def compile(migration: Migration): List[String]
  def defaultTxMode: TxMode = TxMode.Transactional
  def requiresNonTx(migration: Migration): Boolean = false
