package akka.persistence.journal.hbase

import com.stumbleupon.async._
import scala.concurrent.{Promise, Future}

trait DeferredConversions {

  implicit def typedFuture2unitFuture[T](f: Future[T]): Future[Unit] =
    f.asInstanceOf[Future[Unit]]

  implicit def deferred2future(deferred: Deferred[AnyRef]): Future[Unit] = {
    val p = Promise[Unit]()

    val onSuccess = new Callback[AnyRef, AnyRef]{
      def call(in: AnyRef) = p.success(in)
    }

    val onError = new Callback[Any, Exception]{
      def call(ex: Exception) = p.failure(ex)
    }

    deferred
      .addCallback(onSuccess)
      .addErrback(onError)

    p.future
  }

}
