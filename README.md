What is this about?
===

Instead of writing quickcheck-style model based tests which are inneficient and have poor coverage if applied to concurrent systems this approach systematically explores all possible state transitions under certain constraints.

It is somewhat similar to TLA model checker, but it is less generic and has a number of limitations. Approach also similar to [Libra Twins BFT testing]ht(tps://arxiv.org/pdf/2004.10617.pdf), but Twins is applied to more complex problem.

#### Model

This example uses single-round paxos, which is simpler but still complex enough to get it wrong.

Introduction is in [Paxos lecture (Raft case study)](https://www.youtube.com/watch?v=JEpsBg0AO6o).

#### Tests Generator

Generator must be initialized with possible list of replicas, network states (partitions) and leaders. At each step all replicas at the same partition are exchanging messages, if replica is a leader it will start a new ballot (`Paxos.Propose` method).

Generator will create a test case for every possible sequence of steps where each step is uniquely identified as:

- state of the network
- leader (or absence)

#### Tests Runner

Command `go test -run=TestPaxos` will spawn a worker per CPU that will run all available test cases. In case of a failure it will provide a common to re-run a sequence of steps that lead to that error.

For example, if `R1Majority` or `R2Majority` is adjusted to 2 test will fail with a sequence of steps and a tip how to re-run a test `go test -run=TestPaxos -replay=TestPaxos-1614957675921927700.test`.

Beside invalid majorities it is possible to inject other errors, such as forgetting to update ballot after Phase1b or voted value and voted ballot after Phase2B. In all explored failure scenarios model checker is able to find faulty sequence of steps.

#### Options

Other options for tests runner:

```
  -dir string
        directory for replay files. current workdir by default
  -percent int
        percent of the test cases to execute (default 100)
  -replay string
        replay test cases from the file
  -seed int
        seed is used only if percent is less then 100. default is a current time in seconds. (default 1614957754)
  -workers int
        number of workers that will run test cases (default 16)
```
