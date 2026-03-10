# Operational Notes

- Sticky ingress improves owner-hit rate for poll traffic.
- Tune broker lease TTL to your failure-detection and recovery targets.
- Choose `persist_after_ms` so durable writes happen before expected poll timeout windows.
- Persistent jobs require a configured persistence store.
- If TiKV config is present but the crate is built without the `tikv` feature, startup should
  fail fast with a configuration error.

For implementation status and detailed verification scenarios, see the persistence sections in this documentation.
