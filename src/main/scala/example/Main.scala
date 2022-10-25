package main

import org.apache.kafka.clients.producer.ProducerRecord
import zhttp.http._
import zhttp.service.Server
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
  implicit val stockSchema = DeriveSchema.gen[Stock]
  implicit val movementSchema = DeriveSchema.gen[Movement]
  implicit val uuidSchema = Schema.primitive(StandardType.UUIDType)

  implicit def serialize[A](value: A)(implicit schema: Schema[A]) = {
    encode(schema)(value).toArray
  }

  implicit def serializeZIO[A](value: A)(implicit schema: Schema[A]) = {
    ZIO.succeed(serialize(value))
  }

  implicit def deserialize[A](
      value: Chunk[Byte]
  )(implicit schema: Schema[A]) = {
    decode(schema)(value)
  }

  implicit def deserializeZIO[A](
      value: Chunk[Byte]
  )(implicit schema: Schema[A]) = {
    ZIO.fromEither(decode(schema)(value))
  }

  implicit def zioKafkaSerde[A](implicit schema: Schema[A]) =
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
    Stock(symbol = "nflx"),
    Stock(symbol = "glts"),
    Stock(symbol = "amzn")
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
          Console.printLine(
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
      // Preparing RocksDB from backup (or default values if no backup exists)
      _ <- restorePreviousBackup

      // Stock movement simulator
      movementProducer = ZStream
        .repeatZIOChunk(ZIO.succeed(STOCKS))
        .schedule(Schedule.fixed(1.second))
        .mapZIO { stock =>
          for {
            randomChange <- Random.nextDoubleBetween(-20, 20)
            randomUuid <- Random.nextUUID
            _ <- Producer.produceAsync(
              KafkaBackend.TOPIC_NAME,
              randomUuid,
              Movement(stockId = stock.id, change = randomChange),
              zioKafkaSerde[UUID],
              zioKafkaSerde[Movement]
            )
          } yield ()
        }
        .drain

      // Stock movement consumer which stores state in RocksDB
      movementConsumer = Consumer
        .subscribeAnd(Subscription.topics(KafkaBackend.TOPIC_NAME))
        .plainStream(zioKafkaSerde[UUID], zioKafkaSerde[Movement])
        .tap({
          case CommittableRecord(record, _) => {
            Console.printLine(
              s"Consuming Movement Record: (${record.key()}, ${record.value()})"
            )
          }
        })
        .tapSink(
          // Sink that updates stock state based on movements in RocksDB
          ZSink
            .foreach((committableRecord: CommittableRecord[UUID, Movement]) =>
              for {
                stock <- StockService.updateStock(
                  committableRecord.record.value()
                )
                _ <- Producer.produceAsync(
                  KafkaBackend.STOCK_CHANGELOG_TOPIC_NAME,
                  committableRecord.record.key(),
                  stock,
                  zioKafkaSerde[UUID],
                  zioKafkaSerde[Stock]
                )
              } yield ()
            )
        )
        .map(_.offset)
        .aggregateAsync(Consumer.offsetBatches)
        .mapZIO(_.commit)
        .drain

      // Server that receives HTTP requests to access stock values
      // by event-sourcing application
      httpServer = ZStream
        .fromZIO(
          Server.start(
            PORT,
            HttpApi.app @@ (Middleware.debug ++ Middleware.timeout(10.seconds))
          )
        )

      // Merging all into one so if any one of the subsystems crash,
      // the other streams are closed gracefully
      _ <- ZStream
        .mergeAllUnbounded(16)(
          movementProducer,
          movementConsumer,
          httpServer
        )
        .runDrain
    } yield ()

  def run =
    ZIO
      .scoped(scopedApp)
      .provide(RocksDBBackend.database, KafkaBackend.consumerAndProducer)
      .exitCode
}
