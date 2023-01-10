package foorocks

import org.apache.kafka.clients.producer.ProducerRecord
import zio._
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

import java.nio.charset.StandardCharsets
import java.util.UUID

object StockPriceProducerBackend {
  val producer = ZLayer.scoped(
    Producer.make(KafkaBackend.producerSettings)
  )
}

object StockPriceSimulator extends ZIOAppDefault {
  import ImplicitSerde._

  // Stock movement simulator
  def app = ZStream
    .repeatZIOChunk(ZIO.succeed(Main.STOCKS))
    .schedule(Schedule.fixed(100.millis))
    .mapZIO { stock =>
      for {
        randomChange <- Random.nextDoubleBetween(-20, 20)
        randomUuid <- Random.nextUUID
        result <- Producer
          .produceAsync(
            KafkaBackend.TOPIC_NAME,
            randomUuid,
            Movement(stockId = stock.id, change = randomChange),
            zioKafkaSerde[UUID],
            zioKafkaSerde[Movement]
          )
          .logError
      } yield ()
    }
    .runDrain

  def run =
    app
      .provide(StockPriceProducerBackend.producer, Logger.live)
      .exitCode
}
