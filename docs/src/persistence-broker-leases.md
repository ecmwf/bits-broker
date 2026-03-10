# Broker Leases

Broker endpoints are resolved from broker lease records.

Each broker periodically upserts:

- `broker_id`
- internal poll base URL
- lease expiration (`lease_until`)

Renewal runs at approximately half the configured lease TTL.

If a lease is missing or expired, that owner is treated as unavailable for direct proxying and
the recovery path may attempt to claim the persistent job.
