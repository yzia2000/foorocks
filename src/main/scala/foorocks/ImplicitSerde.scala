package foorocks

import org.apache.kafka.clients.producer.ProducerRecord
import zio._
import zio.http._
import zio.http.model._
import zio.kafka._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.kafka.serde._
import zio.rocksdb.RocksDB
import zio.rocksdb.TransactionDB
import zio.schema._
import zio.schema.codec.JsonCodec._
import zio.stream.ZSink
import zio.stream.ZStream
import zio.logging.backend.SLF4J
import java.nio.charset.StandardCharsets
import java.util.UUID
import zio.schema.codec.DecodeError
import zio.logging.LogFormat
import zio.logging.slf4j.bridge.Slf4jBridge

object ImplicitSerde {
  given stockSchema: Schema[Stock] = DeriveSchema.gen[Stock]
  given movementSchema: Schema[Movement] = DeriveSchema.gen[Movement]

  def serialize[A](
      value: A
  )(using schema: Schema[A]): Array[Byte] = {
    schemaBasedBinaryCodec[A].encode(value).toArray
  }

  given implicitSerialization[A: Schema]: Conversion[A, Array[Byte]] =
    serialize(_)

  def serializeZIO[A](
      value: A
  )(using schema: Schema[A]): ZIO[Any, Nothing, Array[Byte]] = {
    ZIO.succeed(serialize(value))
  }

  given implicitSerializationZIO[A: Schema]
      : Conversion[A, ZIO[Any, Nothing, Array[Byte]]] =
    serializeZIO(_)

  def deserialize[A](
      value: Chunk[Byte]
  )(using schema: Schema[A]): Either[DecodeError, A] = {
    schemaBasedBinaryCodec[A].decode(value)
  }

  given implicitDeserialization[A: Schema]
      : Conversion[Chunk[Byte], Either[DecodeError, A]] =
    deserialize(_)

  def deserializeZIO[A](
      value: Chunk[Byte]
  )(using schema: Schema[A]): IO[DecodeError, A] = {
    ZIO.fromEither(deserialize[A](value))
  }

  given implicitDeserializationZIO[A: Schema]
      : Conversion[Chunk[Byte], IO[DecodeError, A]] =
    deserializeZIO(_)

  def zioKafkaSerde[A](using schema: Schema[A]): Serde[Any, A] =
    Serde.byteArray.inmapM(bytes =>
      deserializeZIO[A](Chunk.fromArray(bytes)).mapError(new Exception(_))
    )(serializeZIO _)
}
