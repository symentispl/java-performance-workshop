# How to run benchmarks?

All configurations and benchmark are in Taskfile.yaml (use taskfile.dev).

Use `task run-benchmarks`, to run all benchmarks, and `task profile-benchmarks` 
to run with profilers.

## Implementations

All implementations are in separate branches:

* master - sequential map/reduce
* parallel - parallel map/reduce
* batching
* forkjoin
* virtualthread
