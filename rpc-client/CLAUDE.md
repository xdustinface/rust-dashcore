# rpc-client crate

JSON-RPC client for Dash Core nodes.

## Key Types

- `Client` — main RPC client
- `RpcApi` trait — defines all RPC methods
- `Auth` — authentication enum
- `Queryable<C: RpcApi>` — generic query trait

## Usage

Thin wrapper around `jsonrpc` crate. Type-safe method calls through `RpcApi` trait. Response types come from `rpc-json` crate.
