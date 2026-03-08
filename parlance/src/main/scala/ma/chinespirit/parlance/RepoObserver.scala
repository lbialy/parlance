package ma.chinespirit.parlance

trait RepoObserver[EC, E]:
  def creating(entity: EC | E)(using DbCon[?]): Unit = ()
  def created(entity: E)(using DbCon[?]): Unit = ()
  def updating(entity: E)(using DbCon[?]): Unit = ()
  def updated(entity: E)(using DbCon[?]): Unit = ()
  def deleting(entity: E)(using DbCon[?]): Unit = ()
  def deleted(entity: E)(using DbCon[?]): Unit = ()
  // Soft-delete specific (only fired when SoftDeletes mixin is used)
  def trashed(entity: E)(using DbCon[?]): Unit = ()
  def forceDeleting(entity: E)(using DbCon[?]): Unit = ()
  def forceDeleted(entity: E)(using DbCon[?]): Unit = ()
  def restoring(entity: E)(using DbCon[?]): Unit = ()
  def restored(entity: E)(using DbCon[?]): Unit = ()
