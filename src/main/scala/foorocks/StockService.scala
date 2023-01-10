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
  import ImplicitSerde._

  def addStock(stock: Stock) = {
    for {
      _ <- ZIO.unit
      _ <- rocksdb.TransactionDB.put(
        stock.id,
        stock
      )
    } yield ()
  }

  def updateStock(movement: Movement) = {
    for {
      _ <- ZIO.unit
      result <- TransactionDB.atomically {
        for {
          byteOption <- rocksdb.Transaction.getForUpdate(
            movement.stockId,
            true
          )
          bytes <- ZIO.fromOption(byteOption)
          stock <- deserializeZIO[Stock](Chunk.fromArray(bytes))
          updatedStock = stock.copy(price = stock.price + movement.change)
          _ <- rocksdb.Transaction.put(
            stock.id,
            updatedStock
          )
        } yield (updatedStock)
      }
    } yield (result)
  }

  def getStock(id: UUID) = {
    for {
      _ <- ZIO.unit
      bytesOption <- RocksDB
        .get(
          id
        )
      bytes <- ZIO.fromOption(bytesOption)
      stock <- deserializeZIO[Stock](Chunk.fromArray(bytes))
    } yield (stock)
  }
}

