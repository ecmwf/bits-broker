# Operational Notes

- Prefer sticky ingress for efficiency, but rely on BITS recovery for correctness.
- Tune lock TTL and broker lease TTL to your failure-detection and recovery targets.
- Persistent jobs require a configured persistence store.
- If TiKV config is present but the crate is built without the `tikv` feature, startup should
  fail fast with a configuration error.

For implementation status and detailed verification scenarios, see `PERSISTENCE.md`.
