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

