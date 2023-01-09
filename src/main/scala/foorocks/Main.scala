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

import java.nio.charset.StandardCharsets
import java.util.UUID
import zio.schema.codec.DecodeError

object KafkaBackend {
  val KAFKA_BOOTSTRAP_SERVER = "localhost:29092"
  val GROUP = "foorocks"
  val TOPIC_NAME = "movements"
  val STOCK_CHANGELOG_TOPIC_NAME = "stocks-changelog"

  val consumerSettings: ConsumerSettings =
    ConsumerSettings(List(KAFKA_BOOTSTRAP_SERVER))
      .withGroupId(GROUP)
      .withOffsetRetrieval(
        Consumer.OffsetRetrieval.Auto(Consumer.AutoOffsetStrategy.Earliest)
      )

  val producerSettings: ProducerSettings = ProducerSettings(
    List(KAFKA_BOOTSTRAP_SERVER)
  )

  val consumerAndProducer = ZLayer.scoped(
    Producer.make(producerSettings)
  ) ++ ZLayer.scoped(Consumer.make(consumerSettings))
}

object RocksDBBackend {
  val ROCKSDB_LOCAL_DIR = "./data"

  lazy val database =
    TransactionDB.live(
      new org.rocksdb.Options().setCreateIfMissing(true),
      ROCKSDB_LOCAL_DIR
    )
}

case class Stock(
    id: UUID = UUID.randomUUID(),
    symbol: String,
    price: BigDecimal = 0
);

case class Movement(
    stockId: UUID,
    change: BigDecimal = 0
);

object ImplicitSerde {
  implicit val stockSchema: Schema[Stock] = DeriveSchema.gen[Stock]
  implicit val movementSchema: Schema[Movement] = DeriveSchema.gen[Movement]

  implicit def serialize[A](
      value: A
  )(implicit schema: Schema[A]): Array[Byte] = {
    schemaBasedBinaryCodec[A].encode(value).toArray
  }

  implicit def serializeZIO[A](
      value: A
  )(implicit schema: Schema[A]): ZIO[Any, Nothing, Array[Byte]] = {
    ZIO.succeed(serialize(value))
  }

  implicit def deserialize[A](
      value: Chunk[Byte]
  )(implicit schema: Schema[A]): Either[DecodeError, A] = {
    schemaBasedBinaryCodec[A].decode(value)
  }

  implicit def deserializeZIO[A](
      value: Chunk[Byte]
  )(implicit schema: Schema[A]): IO[DecodeError, A] = {
    ZIO.fromEither(deserialize[A](value))
  }

  implicit def zioKafkaSerde[A](implicit schema: Schema[A]): Serde[Any, A] =
    Serde.byteArray.inmapM(bytes =>
      deserializeZIO[A](Chunk.fromArray(bytes)).mapError(new Exception(_))
    )(serializeZIO _)
}

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

object HttpApi {
  import ImplicitSerde._

  case class StockRecordNotFound(id: String)
      extends Exception(s"Could not find stock: ${id}")

  val app: HttpApp[RocksDB, StockRecordNotFound] =
    Http.collectZIO[Request] { case Method.GET -> !! / "stock" / id =>
      for {
        stock <- StockService
          .getStock(UUID.fromString(id))
          .orElseFail(StockRecordNotFound(id))
      } yield Response.json(new String(serialize(stock)))
    }
}

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
                  _ <- Console.printLine(stocks)
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
        RocksDBBackend.database,
        KafkaBackend.consumerAndProducer,
        Server.default
      )
      .exitCode
}
