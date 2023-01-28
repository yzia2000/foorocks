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

object StockService {
  def addStock(stock: Stock) = {
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
  ): ZIO[RocksDB, Object, Stock] = {
    for {
      stock <- getStock(
        movement.stockId
      )
      updatedStock = stock.copy(price = stock.price + movement.change)
      _ <- putStock(updatedStock)
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

  def getStock(id: UUID): ZIO[RocksDB, Object, Stock] = {
    val keySerde = summon[foorocks.Serde[UUID]]
    val valueSerde = summon[foorocks.Serde[Stock]]
    for {
      bytesOption <- RocksDB
        .get(
          keySerde.serialize(id)
        )
      bytes <- ZIO.fromOption(bytesOption)
      stock <- valueSerde.deserializeZIO(bytes)
    } yield (stock)
  }
}
