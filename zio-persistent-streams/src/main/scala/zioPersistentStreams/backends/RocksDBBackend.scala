package zioPersistentStreams.backends

import zio.rocksdb.TransactionDB
import zio._
import zio.stream._
import zioPersistentStreams.Serde
import zioPersistentStreams.ZTableService

case class RocksDBBackend[K, V](db: TransactionDB) extends ZTableService[K, V] {
  def get(
      key: K
  )(using keySerde: Serde[K], valueSerde: Serde[V]): IO[Any, V] =
    for {
      bytesO <- db.get(keySerde.serialize(key).toArray)
      bytes <- ZIO.fromOption(bytesO)
      value <- ZIO
        .fromEither(
          valueSerde.deserialize(Chunk.fromArray(bytes))
        )
    } yield (value)

  def put(
      key: K,
      value: V
  )(using keySerde: Serde[K], valueSerde: Serde[V]): Task[Unit] =
    db.put(keySerde.serialize(key).toArray, valueSerde.serialize(value).toArray)
}

object RocksDBBackend {
  def live: ZIO[TransactionDB, Nothing, RocksDBBackend[Nothing, Nothing]] =
    ZIO.serviceWith[TransactionDB](RocksDBBackend(_))

  def pipeline[K: Serde, V: Serde](backend: RocksDBBackend[K, V]) = {
    ZPipeline.map { case (key, value): (K, V) =>
      backend.put(key, value) *> ZIO.succeed(value)
    }
  }
}
