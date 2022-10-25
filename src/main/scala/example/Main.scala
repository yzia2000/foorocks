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

  val consumerSettings: ConsumerSettings =
    ConsumerSettings(List(KAFKA_BOOTSTRAP_SERVER))
      .withGroupId(GROUP)
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
          _ <- rocksdb.Transaction.put(
            stock.id,
            stock.copy(price = stock.price + movement.change)
          )
        } yield ()
      }
    } yield ()
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

object App {
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

  def scopedApp =
    for {
      _ <- ZIO.unit

      // Add base Stock values to RocksDB
      _ <- ZStream
        .fromChunk(STOCKS)
        .mapZIO { stock =>
          StockService.addStock(stock)
        }
        .runDrain

      // Stock movement simulator
      producer = ZStream
        .repeatZIOChunk(ZIO.succeed(STOCKS))
        .schedule(Schedule.fixed(1.second))
        .mapZIO { stock =>
          for {
            randomChange <- Random.nextDoubleBetween(-1000, 1000)
            _ <- Producer.produce(
              KafkaBackend.TOPIC_NAME,
              stock.id,
              Movement(stockId = stock.id, change = randomChange),
              zioKafkaSerde[UUID],
              zioKafkaSerde[Movement]
            )
          } yield ()
        }
        .drain

      // Stock movement consumer which stores state in RocksDB
      consumer = Consumer
        .subscribeAnd(Subscription.topics(KafkaBackend.TOPIC_NAME))
        .plainStream(zioKafkaSerde[UUID], zioKafkaSerde[Movement])
        .tap({
          case CommittableRecord(record, _) => {
            Console.printLine(
              record.value()
            )
          }
        })
        .tapSink(
          // Sink that updates stock state based on movements in RocksDB
          ZSink
            .foreach((committableRecord) => {
              StockService.updateStock(
                committableRecord.record.value()
              )
            })
        )
        .map(_.offset)
        .aggregateAsync(Consumer.offsetBatches)
        .mapZIO(_.commit)
        .drain

      // Server that receives HTTP requests to access stock values
      // by event-sourcing application
      server = ZStream
        .fromZIO(
          Server.start(
            PORT,
            App.app @@ (Middleware.debug ++ Middleware.timeout(10.seconds))
          )
        )
      _ <- ZStream
        .mergeAllUnbounded(16)(
          producer,
          consumer,
          server
        )
        .runDrain
    } yield ()

  def run =
    ZIO
      .scoped(scopedApp)
      .provide(RocksDBBackend.database, KafkaBackend.consumerAndProducer)
      .exitCode
}
