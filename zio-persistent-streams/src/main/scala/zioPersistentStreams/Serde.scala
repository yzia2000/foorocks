package zioPersistentStreams

import zio.rocksdb.TransactionDB
import java.util.UUID
import zio._

trait Serde[A] {
  def serialize(x: A): Chunk[Byte]

  def deserialize(xs: Chunk[Byte]): Either[Throwable, A]
}
