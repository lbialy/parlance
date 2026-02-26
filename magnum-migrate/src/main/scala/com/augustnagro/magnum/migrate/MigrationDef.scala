package com.augustnagro.magnum.migrate

trait MigrationDef:
  def version: Long
  def name: String
  def up: List[Migration]
  def down: List[Migration]
  def txMode: TxMode = TxMode.Auto
