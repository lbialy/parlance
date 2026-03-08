package ma.chinespirit.parlance.examples.ecommerce

import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.PgCodec.given
import ma.chinespirit.parlance.pg.enums.given

// --- Observer: audit log for order lifecycle ---

class OrderAuditObserver extends RepoObserver[OrderCreator, Order]:
  override def created(entity: Order)(using DbCon[?]): Unit =
    println(s"[audit] Order #${entity.id} placed by customer ${entity.customerId}, total: ${entity.totalAmount}")

  override def updating(entity: Order)(using DbCon[?]): Unit =
    println(s"[audit] Order #${entity.id} updating")

  override def deleting(entity: Order)(using DbCon[?]): Unit =
    println(s"[audit] Order #${entity.id} being cancelled")

// --- Application context: runtime DI container ---
//
// Repos that need runtime-injected observers live here.
// Companion objects use `inline given` to delegate resolution through AppContext,
// preserving auto-resolution at call sites while allowing runtime injection.
//
// In a real app, a macro-DIC (e.g. macwire) could resolve its dependencies and inject it where needed.

class AppContext(
    orderObservers: Vector[RepoObserver[OrderCreator, Order]] = Vector(OrderAuditObserver())
):
  val orderRepo: Repo[OrderCreator, Order, Long] =
    Repo[OrderCreator, Order, Long](observers = orderObservers)
