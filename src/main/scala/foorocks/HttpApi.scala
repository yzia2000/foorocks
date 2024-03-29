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

object HttpApi {
  case class StockRecordNotFound(id: String)
      extends Exception(s"Could not find stock: ${id}")

  val app: HttpApp[RocksDB, StockRecordNotFound] =
    Http.collectZIO[Request] { case Method.GET -> !! / "stock" / id =>
      for {
        stock <- StockService
          .getStock(UUID.fromString(id))
          .some
          .orElseFail(StockRecordNotFound(id))
      } yield Response.json(
        new String(summon[foorocks.Serde[Stock]].serialize(stock))
      )
    }
}
