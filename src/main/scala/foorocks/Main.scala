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

case class Stock(
    id: UUID = UUID.randomUUID(),
    symbol: String,
    price: BigDecimal = 0
);

case class Movement(
    stockId: UUID,
    change: BigDecimal = 0
);

object Main extends ZIOAppDefault {
  import ImplicitSerde._

  val PORT = 5050

  val STOCKS = Chunk(
    Stock(
      id = UUID.fromString("cf378884-1f4c-4055-9375-8b6634d7c6fc"),
      symbol = "nflx"
    ),
    Stock(
      id = UUID.fromString("26efb03d-3ab6-4927-aaa6-d001072c9559"),
      symbol = "glts"
    ),
    Stock(
      id = UUID.fromString("8a4b159b-63a6-4523-82ef-76d52054fa2b"),
      symbol = "amzn"
    )
  )

  def restorePreviousBackup = for {
    // Add base Stock values to RocksDB
    _ <- ZStream
      .fromChunk(STOCKS)
      .mapZIO { stock =>
        StockService.addStock(stock)
      }
      .runDrain

    // KafkaBackend.STOCK_CHANGELOG_TOPIC_NAME contains a backup of the
    // RocksDB data. Read this backup so in case application previously
    // crashed, RocksDB stocks state is recreated.
    _ <- Consumer
      .subscribeAnd(
        Subscription.topics(KafkaBackend.STOCK_CHANGELOG_TOPIC_NAME)
      )
      .plainStream(zioKafkaSerde[UUID], zioKafkaSerde[Stock])
      .tap({
        case CommittableRecord(record, _) => {
          ZIO.logInfo(
            s"Consuming Stock ChangeLog Record: (${record.key()}, ${record.value()})"
          )
        }
      })
      .tapSink(
        // Sink that updates stock state based on movements in RocksDB
        ZSink
          .foreach((committableRecord: CommittableRecord[UUID, Stock]) =>
            StockService.addStock(
              committableRecord.record.value()
            )
          )
      )
      .map(_.offset)
      .aggregateAsync(Consumer.offsetBatches)
      .mapZIO(_.commit)
      .timeout(10.seconds)
      .runDrain
  } yield ()

  def scopedApp =
    for {
      _ <- ZIO.unit
      // Preparing RocksDB from backup (or default values if no backup exists)
      _ <- restorePreviousBackup

      // Stock movement consumer which stores state in RocksDB
      movementConsumer = Consumer
        .subscribeAnd(Subscription.topics(KafkaBackend.TOPIC_NAME))
        .plainStream(zioKafkaSerde[UUID], zioKafkaSerde[Movement])
        .aggregateAsyncWithin(
          ZSink.collectAllN[CommittableRecord[UUID, Movement]](10000),
          Schedule.fixed(1.seconds)
        )
        .tap(records =>
          ZIO.logInfo(
            s"Consuming Movement Record: ${records.map(_.record.value)}"
          )
        )
        .tapSink(
          // Sink that updates stock state based on movements in RocksDB
          ZSink
            .foreach(
              (movementRecords: Chunk[CommittableRecord[UUID, Movement]]) =>
                for {
                  _ <- ZIO.unit
                  movements = movementRecords.map(_.value)
                  combinedMovements = movements
                    .groupBy(_.stockId)
                    .map { case (key, value) =>
                      value.reduce[Movement]((left, right) =>
                        left.copy(change = left.change + right.change)
                      )
                    }
                  stocks <- ZIO.foreach(combinedMovements)(
                    StockService.updateStock(
                      _
                    )
                  )
                  _ <- ZIO.foreach(stocks)(stock =>
                    Producer.produceAsync(
                      KafkaBackend.STOCK_CHANGELOG_TOPIC_NAME,
                      stock.id,
                      stock,
                      zioKafkaSerde[UUID],
                      zioKafkaSerde[Stock]
                    )
                  )
                  _ <- ZIO.logInfo(stocks.toString())
                } yield ()
            )
        )
        .mapZIO(_.map(_.offset).collectZIO(_.commit))
        .drain

      // Server that receives HTTP requests to access stock values
      // by event-sourcing application
      httpServer = ZStream.fromZIO(
        Server
          .serve(
            HttpApi.app @@ (Middleware.debug ++ Middleware
              .timeout(10.seconds))
          )
      )

      // Merging all into one so if any one of the subsystems crash,
      // the other streams are closed gracefully
      _ <- ZStream
        .mergeAllUnbounded(16)(
          movementConsumer,
          httpServer
        )
        .runDrain
    } yield ()

  def run =
    ZIO
      .scoped(scopedApp)
      .provide(
        Logger.live,
        RocksDBBackend.database,
        KafkaBackend.consumerAndProducer,
        Server.default
      )
      .exitCode
}
