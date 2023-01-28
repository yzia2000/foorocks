package foorocks

import java.util.UUID

import Serde.given

case class Stock(
    id: UUID = UUID.randomUUID(),
    symbol: String,
    price: BigDecimal = 0
) derives Serde
