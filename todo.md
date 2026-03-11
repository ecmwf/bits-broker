- [x] Figure out how to make the HTTP frontend more customisable without duplicating the request/poll logic
        * The HTTP frontend is now implemented by each service, BITS only provides a library but includes request/poll logic.
- [x] Dispatcher architecture — Queue + Executor composable at route-step level for any Check, Transform, or Target
- [x] External worker pool (`target::remote` + `executor: remote_pool`) — stub implemented; HTTP long-poll worker API and result callback not yet done
- [x] Add persistence, maybe time-based
- [ ] Add integration tests which actually spin up tikv
- [ ] Build an FDB worker (new repo)
- [ ] Add an age-based priority queue
- [ ] Add a throttle-proxy
- [ ] Implement a MARS library and worker (new repo)
- [ ] Allow dynamic configuration of routes
- [ ] Clean up unwraps and expect, add better error handling
- [ ] Migrate rules from polytope to BITS
- [ ] Add tier-based token bucket implementation for rate-limiting
- [ ] Consider anonymous access
- [ ] Load balancing of workers to brokers
- [ ] Broker lease should be a thread, not an async task
- [ ] Diagrams in the docs

- [ ] Add ecmwf-specific match actions, schedule actions and authotron action
- [ ] Think about metrics implementation, aggregate statistics

# Human TODO:
- [ ] Set up a k8s cluster for prototype of polytope

# Advanced
- [ ] Tiered token bucket implementation for rate-limiting
- [ ] Dehogger implementation to handle access to shared resources
- [ ] For slow jobs we might want a globally-synchronised queue


# Optional:
- [ ] Allow compile to WASM for interactive docs
- [ ] Add a GUI for pipeline configuration and monitoring