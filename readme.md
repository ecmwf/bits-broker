# B.I.T.S. — Broker for Intelligent Task Scheduling

Most scalable job queues, or brokers, assume FIFO semantics and minimal decision logic. They perform well because the queue logic is simple and uniform. But as soon as queueing behaviour depends on job attributes — such as age, cost, resource requirements, or other constraints such as user quotas — the logic becomes more complex, and the broker becomes a computational bottleneck. Scaling a broker is a hard problem, which B.I.T.S. is designed to solve.


## Design

* The queue is sharded arbitrarily among multiple brokers. Each broker has a partial view of the queue.
* The queue logic is custom, and defined in software on an in-memory view of the queue. It can be adapted for different use-cases.
* The broker periodically reports to a central resource-tracking database with its _resource pressure_, a metric stating how much of each given resource is being demanded.
* The brokers negotiate, using deterministic logic, how much of the resource quota they are allowed. They lease this reservation from the resource tracker.
* Workers connect to the brokers to ask for work (pull). The broker gives them work within the broker's leased resource allowance. They block jobs which cannot be scheduled immediately.

* User statistics (mainly usage) are tracked in a central database. For persisent jobs, usage is tracked per-job. For ephemeral jobs, usage stats are aggregated and batched.
* To avoid querying the database for each user request, brokers reserve an allocation of a user's quota and manage it locally.

## Persistence

Each task can have optional persistence. If the task is flagged as persistent, then it is committed to the _task database_ at every stage of its lifetime. If it is ephemeral, then it exists only in-memory of a particular broker. Both can be supported at the same time, allowing for cheap jobs to have higher throughput while more expensive jobs are persisted.

## Failure Modes

* Broker loss:
  * Connected workers abandon all jobs and free their resources.
  * Resource lease of the broker expires. Brokers begin redistributing the resources.
  * Persistent jobs are queued onto alternative brokers.
 
* Database loss:
  * A high availability database is recommended, so that failure of individual nodes can be tolerated.
  * Loss of the entire database means brokers cannot negotiate resources.
  * It cannot safely keep its lease of resources. It does not know if the database is unavailable or if has become disconnected (similar to a broker loss).
  * All incoming jobs should be rejected. Jobs in flight may continue, but cannot be persisted.
 
* Worker loss:
  * When a worker has no task, this has no considerable impact.
  * When it has a task, the broker no longer receives the heartbeat from the worker.
    * If the job was ephemeral, the broker does nothing. The job is lost.
    * If the job is persistent, it tries to reschedule it.
   
* Poisoned task:
  * If the content of the task is causing the worker to terminate (e.g. it consumes too much memory) it is hard to distinguish from worker loss.
    * For persisent jobs, a retry count is incremented on every retry. After a configurable number of retries the task is dropped.
  * If the content of the task is causing the worker to hang indefinetely (but not unresponsive, it's still sending a heartbeat):
    * An optional timeout can terminate these tasks.
    * Task response time metrics are also collected, and should also be used for observability.
   
  * Network partition:
    * Any broker not connected to the database is considered as a broker loss.
    * Any broker still connected can pick up the resource allocation and any persistent jobs which are no longer assigned.
    * The queue can continue functioning with reduced throughput.
    * If the database loses quorum, the service is lost.

# Progress

- [ ] Build the resource-sharing logic based on a basic local database or filesystem
- [ ] Build a basic visualisation of the resource partitioning
- [ ] Build a basic queue loop
- [ ] Mock producer and consumer
- [ ] Demonstrate correct behaviour of scaling up/down the amount of resources

## Longer Term

- [ ] User statistics
- [ ] Persistence and rescheduling
