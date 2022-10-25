# foorocks

Foo really rocks. Example of using zio-http, zio-kafka, zio-schema, zio-rocksdb, zio-streams and
a bunch of Scala libraries (especially zio ecosystem) to create an event
sourcing application.

## Entities

There are two entities: `Stock` and `Movement`. There are a few fixed stocks like `amzn` (Amazon),
`nflx` (Netflix) and `glts` (Glints).

```scala
case class Stock(
    id: UUID = UUID.randomUUID(),
    symbol: String,
    price: BigDecimal = 0
);
```

Movements are changes in price to the stock.

```scala
case class Movement(
    stockId: UUID,
    change: BigDecimal = 0
);
```

So a movement with `stockId` for `nflx` with `change=10` ie `Movement("nflx-uuid", 10)`, is an event
which when consumed, changes the nflx stock by `+10` ie `nflxStock.price = nflxStock.price + 10`.

The goal is to create an application that can receive these movements at high frequencies and update the
stock correctly with low latency and good throughput.

## External Components

### Kafka

Kafka is the event bus (or a distributed commit log) that receives, stores and shares `Movement` data
for stocks. It is also/will be used for changelog data to the `Stock` so when our application goes down
and comes back up, we start where we left off.

### RocksDB

Technically not externally but more so embedded; This is the store we use to read and write stateful `Stock`
data. Changes to `Stock` after consuming `Movement` will be persisted via this db. RocksDB is used for persistence
and because it is embedded, it is fast. Not to mention the configurable block cache and the memtables.

## Internal Components

### Simulator/Movement Producer

This is a Kafka producer that generates random `Movement` data and produces it to Kafka.

### Consumer

Consumes `Movement` produced by the Simulator and changes the stock price, storing it in `RocksDB`
(and change logging into Kafka in the future)

### Web API/App

zio-http application that is used to retrieve the stock data by id from the state store.

## Setup

### Requirements

1. docker
1. docker-compose
1. Java (preferably Java 17). If you are using M1 Mac, use SDKMAN and java 17.0.5-zulu
1. sbt

### Setup External Components

```
docker-compose up -d broker
```

### Run application

```
sbt run
```
