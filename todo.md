- [x] Figure out how to make the HTTP frontend more customisable without duplicating the request/poll logic
        * The HTTP frontend is now implemented by each service, BITS only provides a library but includes request/poll logic.
- [x] Dispatcher architecture — Queue + Executor composable at route-step level for any Check, Transform, or Target
- [~] External worker pool (`target::remote` + `executor: remote_pool`) — stub implemented; HTTP long-poll worker API and result callback not yet done
- [ ] Add persistence
- [ ] Build an FDB worker (new repo)
- [ ] Allow dynamic configuration of routes

- [ ] Add ecmwf-specific match actions, schedule actions and authotron action

# Advanced
- [ ] Tiered token bucket implementation for rate-limiting
- [ ] Dehogger implementation to handle access to shared resources
