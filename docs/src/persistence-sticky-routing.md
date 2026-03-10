# Sticky Routing

Ingress stickiness (for example hashing on `Authorization`) is recommended for performance,
because most polls then return directly from the owning broker.

BITS handles wrong-broker polls by internal proxying using broker lease records.

When a lease is missing or expired, the broker can claim and recover persistent jobs from durable
storage.
