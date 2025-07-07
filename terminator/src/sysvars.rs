use solana_sdk::{clock::Clock, sysvar::SysvarId};
use anyhow::Result;
use orbit_link::async_client::AsyncClient;
use tracing::error;

/// Get current clock
pub async fn get_clock(rpc: &impl AsyncClient) -> Result<Clock> {
    let clock = rpc.get_account(&Clock::id()).await?.deserialize_data()?;

    Ok(clock)
}

/// Get current clock
pub async fn clock(rpc: &impl AsyncClient) -> Result<Clock> {
    match get_clock(rpc).await {
        Ok(clock) => Ok(clock),
        Err(e) => {
            error!("Failed to get clock: {}", e);
            Err(e)
        }
    }
}
