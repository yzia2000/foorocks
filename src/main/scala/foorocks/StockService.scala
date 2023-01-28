package foorocks

import org.apache.kafka.clients.producer.ProducerRecord
import zio._
import zio.http._
import zio.http.model._
import zio.kafka._
import zio.kafka.consumer._
import zio.kafka.producer._
import zio.kafka.serde._
import zio.logging.LogFormat
import zio.logging.backend.SLF4J
import zio.logging.slf4j.bridge.Slf4jBridge
import zio.rocksdb.RocksDB
import zio.rocksdb.TransactionDB
import zio.schema._
import zio.schema.codec.DecodeError
import zio.schema.codec.JsonCodec._
import zio.stream.ZSink
import zio.stream.ZStream

import java.nio.charset.StandardCharsets
import java.util.UUID

object StockService {
  def addStock(stock: Stock): ZIO[TransactionDB, Throwable, Unit] = {
    val keySerde = summon[foorocks.Serde[UUID]]
    val valueSerde = summon[foorocks.Serde[Stock]]
    for {
      _ <- ZIO.unit
      _ <- rocksdb.TransactionDB.put(
        keySerde.serialize(stock.id),
        valueSerde.serialize(stock)
      )
    } yield ()
  }

  def updateStock(
      movement: Movement
  ): ZIO[RocksDB, Throwable, Option[Stock]] = {
    for {
      stock <- getStock(
        movement.stockId
      )
      updatedStock = stock.map(stock =>
        stock.copy(price = stock.price + movement.change)
      )
      _ <- updatedStock match
        case None => ZIO.none
        case Some(updatedStock) =>
          putStock(updatedStock)
    } yield (updatedStock)
  }

  def putStock(stock: Stock): ZIO[RocksDB, Throwable, Unit] = {
    val keySerde = summon[foorocks.Serde[UUID]]
    val valueSerde = summon[foorocks.Serde[Stock]]
    for {
      _ <- RocksDB.put(
        keySerde.serialize(stock.id),
        valueSerde.serialize(stock)
      )
    } yield ()
  }

  def getStock(id: UUID): ZIO[RocksDB, Throwable, Option[Stock]] = {
    val keySerde = summon[foorocks.Serde[UUID]]
    val valueSerde = summon[foorocks.Serde[Stock]]
    for {
      bytesOption <- RocksDB
        .get(
          keySerde.serialize(id)
        )
      result <- bytesOption match
        case None        => ZIO.none
        case Some(bytes) => valueSerde.deserializeZIO(bytes).map(Some.apply)
    } yield (result)
  }
}
