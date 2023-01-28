package foorocks

import zio._
import zio.kafka
import zio.schema._
import zio.schema.codec.DecodeError
import zio.schema.codec.JsonCodec._

import java.util.UUID

trait Serde[A] {
  def serialize(x: A): Array[Byte]

  def deserialize(xs: Array[Byte]): Either[Throwable, A]

  def serializeZIO(
      value: A
  ): ZIO[Any, Nothing, Array[Byte]]

  def deserializeZIO(
      value: Array[Byte]
  ): IO[Throwable, A]
}

object Serde {
  inline given schema[A]: Schema[A] = DeriveSchema.gen

  given derived[A](using Schema[A]): Serde[A] with {
    def serialize(x: A): Array[Byte] =
      schemaBasedBinaryCodec[A].encode(x).toArray

    def deserialize(
        value: Array[Byte]
    ): Either[Throwable, A] = {
      schemaBasedBinaryCodec[A].decode(Chunk.fromArray(value))
    }

    def serializeZIO(
        value: A
    ): ZIO[Any, Nothing, Array[Byte]] = {
      ZIO.succeed(serialize(value))
    }

    def deserializeZIO(
        value: Array[Byte]
    ): IO[Throwable, A] = {
      ZIO.fromEither(deserialize(value))
    }
  }

  def zioKafkaSerde[A](using
      serde: Serde[A]
  ): kafka.serde.Serde[Any, A] =
    zio.kafka.serde.Serde.byteArray.inmapM(bytes =>
      serde.deserializeZIO(bytes).mapError(new Exception(_))
    )(serde.serializeZIO _)
}
