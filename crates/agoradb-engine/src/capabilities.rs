// Copyright 2025 The AgoraDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

bitflags::bitflags! {
    /// What a [`QueryEngine`](crate::QueryEngine) is able to do.
    ///
    /// The planner consults these before routing a statement; for example a
    /// `BEGIN` is only sent to engines with [`Capabilities::TRANSACTIONS`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Capabilities: u32 {
        /// Optimised for large scans, joins and aggregates.
        const OLAP = 1;
        /// Optimised for point reads/writes.
        const OLTP = 1 << 1;
        /// Supports `begin` / `commit` / `rollback`.
        const TRANSACTIONS = 1 << 2;
        /// Can read [`TableSource::ParquetFiles`](crate::TableSource::ParquetFiles).
        const READ_PARQUET = 1 << 3;
        /// Can read [`TableSource::SqliteFile`](crate::TableSource::SqliteFile).
        const READ_SQLITE = 1 << 4;
        /// Produces Arrow batches natively (no row conversion).
        const ARROW_OUT = 1 << 5;
        /// Joins between its own tables may be pushed down as one statement.
        const PUSHDOWN_JOIN = 1 << 6;
        /// Aggregates may be pushed down.
        const PUSHDOWN_AGG = 1 << 7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_bitflags_roundtrip() {
        let caps = Capabilities::OLAP | Capabilities::READ_PARQUET | Capabilities::ARROW_OUT;
        assert!(caps.contains(Capabilities::OLAP));
        assert!(!caps.contains(Capabilities::TRANSACTIONS));
        let bits = caps.bits();
        assert_eq!(Capabilities::from_bits(bits), Some(caps));
        assert_eq!(Capabilities::from_bits_truncate(bits | 0xF000), caps);
    }
}
