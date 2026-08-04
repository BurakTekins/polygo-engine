use std::{env, str::FromStr};

use alloy::signers::{
    local::{LocalSigner, PrivateKeySigner},
    Signer as _,
};
use anyhow::{Context, Result};
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Credentials, Normal},
    clob::{
        types::{response::PostOrderResponse, Amount, OrderType, Side as ClobSide, SignatureType},
        Client as ClobClient, Config as ClobConfig,
    },
    types::{Address, Decimal, U256},
    POLYGON,
};
use tracing::info;

use super::math::{decimal_from_f64, floor_to_2_decimals, ElapsedToNowMs};
use crate::market::{now_ms, ActiveMarket};

const DEFAULT_CLOB_API_URL: &str = "https://clob-v2.polymarket.com";

pub(super) type AuthenticatedClobClient = ClobClient<Authenticated<Normal>>;

#[derive(Clone)]
pub struct LiveExecutor {
    pub(super) client: AuthenticatedClobClient,
    signer: PrivateKeySigner,
    pub(super) funder_address: Option<String>,
    pub(super) signature_type: String,
}

impl LiveExecutor {
    pub async fn from_env() -> Result<Self> {
        let host = env_nonempty("POLYMARKET_CLOB_API_URL")
            .or_else(|| env_nonempty("CLOB_API_URL"))
            .unwrap_or_else(|| DEFAULT_CLOB_API_URL.to_owned());
        let private_key = env_nonempty("POLYMARKET_PRIVATE_KEY")
            .or_else(|| env_nonempty("POLYGO_PRIVATE_KEY"))
            .context("POLYMARKET_PRIVATE_KEY is required for live execution")?;
        let signer = LocalSigner::from_str(&private_key)
            .context("invalid POLYMARKET_PRIVATE_KEY")?
            .with_chain_id(Some(POLYGON));

        let mut builder = ClobClient::new(&host, ClobConfig::default())
            .context("failed to create Polymarket CLOB client")?
            .authentication_builder(&signer);

        let key = env_nonempty("POLYMARKET_API_KEY").or_else(|| env_nonempty("POLYGO_API_KEY"));
        let secret =
            env_nonempty("POLYMARKET_API_SECRET").or_else(|| env_nonempty("POLYGO_API_SECRET"));
        let passphrase = env_nonempty("POLYMARKET_API_PASSPHRASE")
            .or_else(|| env_nonempty("POLYGO_API_PASSPHRASE"));
        if let (Some(key), Some(secret), Some(passphrase)) = (key, secret, passphrase) {
            builder = builder.credentials(Credentials::new(
                key.parse().context("invalid POLYMARKET_API_KEY")?,
                secret,
                passphrase,
            ));
        }

        let funder = env_nonempty("POLYMARKET_FUNDER_ADDRESS")
            .or_else(|| env_nonempty("POLYMARKET_DEPOSIT_WALLET"))
            .or_else(|| env_nonempty("DEPOSIT_WALLET"));
        let signature_type = parse_signature_type(
            env_nonempty("POLYMARKET_SIGNATURE_TYPE").as_deref(),
            funder.is_some(),
        )?;
        let funder_address = funder.clone();
        if let Some(funder) = funder {
            builder = builder.funder(Address::from_str(&funder).context("invalid funder address")?);
        }
        let signature_type_label = format!("{signature_type:?}");
        builder = builder.signature_type(signature_type);

        let client = builder
            .authenticate()
            .await
            .context("Polymarket authentication failed")?;
        Ok(Self {
            client,
            signer,
            funder_address,
            signature_type: signature_type_label,
        })
    }

    pub(super) async fn sell_shares(
        &self,
        token_id: U256,
        shares: f64,
    ) -> Result<PostOrderResponse> {
        let sellable_shares = floor_to_2_decimals(shares);
        let started = std::time::Instant::now();
        let response = self
            .client
            .market_order()
            .token_id(token_id)
            .side(ClobSide::Sell)
            .amount(Amount::shares(decimal_from_f64(sellable_shares)?)?)
            .order_type(OrderType::FAK)
            .build_sign_and_post(&self.signer)
            .await;
        info!(
            op = "sell_shares_fak",
            total_ms = started.elapsed().as_millis() as u64,
            success = response
                .as_ref()
                .map(|response| response.success)
                .unwrap_or(false),
            error = response.is_err(),
            "Polymarket live HTTP timing"
        );
        response.context("Polymarket sell order failed")
    }

    pub(super) async fn place_gtc_buy(
        &self,
        token_id: U256,
        shares: u64,
        limit_price: f64,
    ) -> Result<TimedPostOrderResponse> {
        let build_started_ms = now_ms();
        let order = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(ClobSide::Buy)
            .price(decimal_from_f64(limit_price)?)
            .size(Decimal::from(shares))
            .order_type(OrderType::GTC)
            .build()
            .await
            .context("Polymarket GTC buy order build failed")?;
        let build_latency_ms = now_ms().saturating_sub(build_started_ms);
        let sign_started_ms = now_ms();
        let signed = self
            .client
            .sign(&self.signer, order)
            .await
            .context("Polymarket GTC buy order sign failed")?;
        let sign_latency_ms = now_ms().saturating_sub(sign_started_ms);
        let post_started_ms = now_ms();
        let response = self
            .client
            .post_order(signed)
            .await
            .context("Polymarket GTC buy order post failed")?;
        let post_latency_ms = now_ms().saturating_sub(post_started_ms);
        info!(
            op = "place_gtc_buy",
            build_ms = build_latency_ms,
            sign_ms = sign_latency_ms,
            post_ms = post_latency_ms,
            total_ms = build_started_ms.elapsed_to_now_ms(),
            success = response.success,
            status = %response.status,
            "Polymarket live HTTP timing"
        );
        Ok(TimedPostOrderResponse {
            response,
            build_latency_ms,
            sign_latency_ms,
            post_latency_ms,
        })
    }

    pub async fn warm_market_cache(&self, market: &ActiveMarket) -> Result<()> {
        self.client
            .version()
            .await
            .context("Polymarket version warm-up failed")?;
        for token_id_text in [&market.yes_asset_id, &market.no_asset_id] {
            let token_id = U256::from_str(token_id_text).context("invalid warm-up token id")?;
            self.client
                .tick_size(token_id)
                .await
                .context("Polymarket tick size warm-up failed")?;
            self.client
                .neg_risk(token_id)
                .await
                .context("Polymarket neg-risk warm-up failed")?;
        }
        Ok(())
    }

    pub(super) async fn place_gtc_sell(
        &self,
        token_id: U256,
        shares: f64,
        limit_price: f64,
    ) -> Result<PostOrderResponse> {
        let build_started_ms = now_ms();
        let order = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(ClobSide::Sell)
            .price(decimal_from_f64(limit_price)?)
            .size(decimal_from_f64(shares)?)
            .order_type(OrderType::GTC)
            .build()
            .await
            .context("Polymarket GTC sell order build failed")?;
        let build_latency_ms = now_ms().saturating_sub(build_started_ms);
        let sign_started_ms = now_ms();
        let signed = self
            .client
            .sign(&self.signer, order)
            .await
            .context("Polymarket GTC sell order sign failed")?;
        let sign_latency_ms = now_ms().saturating_sub(sign_started_ms);
        let post_started_ms = now_ms();
        let response = self
            .client
            .post_order(signed)
            .await
            .context("Polymarket GTC sell order post failed")?;
        let post_latency_ms = now_ms().saturating_sub(post_started_ms);
        info!(
            op = "place_gtc_sell",
            build_ms = build_latency_ms,
            sign_ms = sign_latency_ms,
            post_ms = post_latency_ms,
            total_ms = build_started_ms.elapsed_to_now_ms(),
            success = response.success,
            status = %response.status,
            "Polymarket live HTTP timing"
        );
        Ok(response)
    }
}

pub(super) struct TimedPostOrderResponse {
    pub(super) response: PostOrderResponse,
    pub(super) build_latency_ms: u64,
    pub(super) sign_latency_ms: u64,
    pub(super) post_latency_ms: u64,
}

fn env_nonempty(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn parse_signature_type(value: Option<&str>, has_funder: bool) -> Result<SignatureType> {
    let value = value.unwrap_or(if has_funder { "3" } else { "0" });
    match value {
        "0" | "EOA" | "eoa" => Ok(SignatureType::Eoa),
        "1" | "PROXY" | "proxy" => Ok(SignatureType::Proxy),
        "2" | "GNOSIS_SAFE" | "gnosis_safe" => Ok(SignatureType::GnosisSafe),
        "3" | "POLY1271" | "poly1271" => Ok(SignatureType::Poly1271),
        _ => anyhow::bail!("invalid POLYMARKET_SIGNATURE_TYPE"),
    }
}
