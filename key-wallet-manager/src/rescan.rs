use core::ops::Range;

use dashcore::prelude::CoreBlockHeight;
use dashcore::Address;
use key_wallet::managed_account::address_pool::AddressPoolType;

use crate::WalletId;

/// A backfill obligation surfaced from a wallet's pending sync ranges.
///
/// `floor` is the wallet's `birth_height` and the absolute lower bound below
/// which scans are pointless. `ceiling` is `since_height - 1` and the
/// inclusive upper bound at which point the range is fully caught up.
/// `resume_from` is `caught_up_to.map(|c| c + 1).max(floor)`, so the
/// backfill worker can pick up exactly where it left off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingRescan {
    pub wallet_id: WalletId,
    pub pool: AddressPoolType,
    pub indexes: Range<u32>,
    pub addresses: Vec<Address>,
    pub floor: CoreBlockHeight,
    pub ceiling: CoreBlockHeight,
    pub resume_from: CoreBlockHeight,
}
