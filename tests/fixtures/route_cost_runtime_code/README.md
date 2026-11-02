# Ethereum V2 runtime-code known answers

These fixture bytes are the exact `eth_getCode` results used by the strict
route-cost market-identity known-answer test. They were captured on 2026-08-12
from `https://eth.drpc.org` at the plan-pinned Ethereum block `20,000,000`
(`0x1312d00`) with these read-only requests:

```text
eth_getCode(0x7a250d5630b4cf539739df2c5dacb4c659f2488d, 0x1312d00)
eth_getCode(0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f, 0x1312d00)
eth_getCode(0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc, 0x1312d00)
eth_getCode(0xa478c2975ab1ea89e8196811f51a7b7ade33eb11, 0x1312d00)
```

The JSON-RPC envelopes were not retained. The lowercase runtime hex was
decoded to binary only after exact JSON-RPC/result grammar checks. To keep the
checked-in fixtures reviewable text, each byte string is stored as a
deterministic gzip stream (`gzip -n`) encoded with base64. The test decodes,
decompresses, and recomputes these identities before using the bytes:

- `uniswap-v2-router02-runtime.bin.gz.b64`: 21,943 decoded bytes,
  SHA-256 `ccef50da4af021b09ada39d78db5d281fffff81a57969c7028bccc1f50d37854`.
- `uniswap-v2-factory-runtime.bin.gz.b64`: 13,859 decoded bytes,
  SHA-256 `3abc53f12a9cb8ae37ebfada9efc261c1ab4c2759d161e341a49bf67df3f8321`.
- `uniswap-v2-pair-runtime.bin.gz.b64`: 11,293 decoded bytes,
  SHA-256 `8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4`.
  The two independent canonical pair addresses returned byte-identical
  runtime code at the fixed block.

No private RPC URL, credential, response header, or mutable `latest` result is
present in this directory.
