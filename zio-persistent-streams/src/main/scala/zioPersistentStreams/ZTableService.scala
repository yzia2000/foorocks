package zioPersistentStreams

import zio.rocksdb.TransactionDB
import java.util.UUID
import zio._

trait ZTableService[K, V] {
  def get(key: K)(using Serde[K], Serde[V]): IO[Any, V]

  def put(key: K, value: V)(using Serde[K], Serde[V]): Task[Unit]
}
