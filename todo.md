- [x] Figure out how to make the HTTP frontend more customisable without duplicating the request/poll logic
        * The HTTP frontend is now implemented by each service, BITS only provides a library but includes request/poll logic.
- [x] Dispatcher architecture — Queue + Executor composable at route-step level for any Check, Transform, or Target
- [x] External worker pool (`target::remote` + `executor: remote_pool`) — stub implemented; HTTP long-poll worker API and result callback not yet done
- [x] Add persistence, maybe time-based
- [ ] Resolve the discussions folder
- [x] Add integration tests which actually spin up tikv
- [x] Build an FDB worker (new repo)
- [x] Add an age-based priority queue
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
- [ ] Stress test
- [ ] Consider memory fragmentation for long-running brokers
- [ ] Review the whole build process. We should build from tags really, but would be good to maintain a development mode which uses local repos. skaffold build logic can be quite sophisiticated for this

- [ ] Add ecmwf-specific match actions, schedule actions and authotron action
- [ ] Think about metrics implementation, aggregate statistics
- [ ] Consider if in-flight jobs should be cancellable
- [x] change /test to /health in v2

- [ ] test stream failure modes: before sending bytes, in the middle of sending bytes, at the end.

# Human TODO:
- [ ] Set up a k8s cluster for prototype of polytope

# Advanced
- [ ] Tiered token bucket implementation for rate-limiting
- [ ] Dehogger implementation to handle access to shared resources
- [ ] For slow jobs we might want a globally-synchronised queue
- [ ] Add a post-processing hook. Consider how this will interact with BOBS and indirect responses.


# Optional:
- [ ] Allow compile to WASM for interactive docs
- [ ] Add a GUI for pipeline configuration and monitoringma


# Fixes

- [ ] mars worker images have not been built or tested
- [ ] tikv cluster not available does not seem to be a hard failure, just WARN in logs?
- [ ] request ID in the polytope-fe-worker includes the broker id
- [ ] In the current execution, fdb worker panics, but client is getting a 200

- [ ] We need integration tests between the workers and the broker: Found it. "The worker is getting 422 Unprocessable Entity when posting error completions." We had an error on serialization of error messages from the worker.

- [ ] The number of forecast_days in the meteoapi seems wrong.