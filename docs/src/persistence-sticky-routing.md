# Sticky Routing and Correctness

Ingress stickiness (for example hashing on `Authorization`) is recommended for performance,
because most polls then return directly from the owning broker.

However, correctness does not depend on stickiness:

- scale changes can move clients between instances
- rollouts can drain or replace owners
- load balancer behavior can shift routing at any time

BITS handles wrong-broker polls internally and recovers when ownership is unavailable, so
clients do not need routing-level correctness assumptions.
