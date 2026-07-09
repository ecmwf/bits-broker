<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Request IDs

BITS request IDs are public, opaque identifiers returned to clients when a job is still running. Clients should store and replay the full string, but should not parse it or use it for routing decisions.

Example:

```text
0217scypcc00000000000000
```

## Encoding

A request ID is a 26-character lowercase Crockford base32 string encoding 16 bytes:

| Bytes | Field | Description |
|---|---|---|
| 0 | Version | Current value: `1`. |
| 1-2 | Site tag | 1-3 character deployment site tag. |
| 3-4 | Environment tag | 1-3 character deployment environment tag. |
| 5-8 | Timestamp | Unsigned seconds since the custom epoch `2025-01-01T00:00:00Z`. |
| 9-10 | Broker slot | Durable `u16` slot allocated for this broker process within the site/env pair. |
| 11-15 | Random | 5 bytes generated from `OsRng`. |

The site and environment tags are configured as `bits.site` and `bits.env`. Tags are 1-3 lowercase letters or digits. They are packed using base37 with a sentinel for unused positions.

## Semantics

The ID is stable for the lifetime of the job and is the only public job handle. Internally, BITS can decode the site, environment, and broker slot to form an owner hint, but that is an implementation detail. Durable job records carry an authoritative `broker_id`, which may change after recovery.

Do not expose decoded fields as user API fields, make routing decisions in clients from them, or assume the encoding will remain meaningful outside BITS internals.
