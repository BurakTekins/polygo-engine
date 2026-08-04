#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    BuyYes,
    BuyNo,
}

impl OrderSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BuyYes => "BUY_YES",
            Self::BuyNo => "BUY_NO",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderSignal {
    pub side: OrderSide,
    pub momentum_usd: f64,
    pub signal_ts_ms: u64,
    pub binance_price: f64,
    pub execute_at: tokio::time::Instant,
    pub config_generation: u64,
    pub hold_ms: u64,
}

impl OrderSignal {
    pub fn new(
        side: OrderSide,
        momentum_usd: f64,
        signal_ts_ms: u64,
        binance_price: f64,
        execute_at: tokio::time::Instant,
        config_generation: u64,
        hold_ms: u64,
    ) -> Self {
        Self {
            side,
            momentum_usd,
            signal_ts_ms,
            binance_price,
            execute_at,
            config_generation,
            hold_ms,
        }
    }
}
