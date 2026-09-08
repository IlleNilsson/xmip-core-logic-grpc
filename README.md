# xmip-core-logic-grpc

The `grpc` logic technology, a technology of
[xmip-core-logic](https://github.com/IlleNilsson/xmip-core-logic): a /package.Service/Method path and a length-prefixed protobuf frame; a result is a frame with grpc-status 0, a fault a non-zero status with its message.

ADR-0043: a Logic technology turns a Stream that arrived on a transport into a
named operation with typed arguments, and an operation's result back into a
Stream, using a contract to type both. Both directions live here: a Receive
Location reads invocations and writes replies, a Send Location writes requests
and reads outcomes.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
