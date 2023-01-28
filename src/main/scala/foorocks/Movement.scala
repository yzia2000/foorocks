package foorocks

import java.util.UUID

import Serde.given

case class Movement(
    stockId: UUID,
    change: BigDecimal = 0
) derives Serde
