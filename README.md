How to write stateful model-based tests (using Paxos as an example)?
===

[TLA spec](https://github.com/tlaplus/Examples/blob/master/specifications/Paxos/Paxos.tla)
[Libra Twins BFT testing](https://arxiv.org/pdf/2004.10617.pdf)

What do we want to get?

1. Exhaustive test suite.
2. Runtime measured in minutes instead of hours.
3. Reproduce bugs deterministically, by re-running test case from a replay log.

#### State

Each replica can choose one of the actions:

1. Time out and propose a new value or do nothing if value is learned. (P1)
2. Send messages or do nothing if messages box is empty. (P2)

At each step connectivity model may change, for example
given 3 replicas `{A, B, C}`.

For 3 replicas we can define next connectivity models:

1. `{A, B, C}`   - fully connected. (C1)
2. `{A} {B, C}`  - proposer disconnected. (C2)
3. `{A, B} {C}`  - proposer connected (C3)

Note that we discarded fully disconnected model (`{A}, {B}, {C}`) as it doesn't produce
cover any additional behavior. And `{A, C} {B}` was discarded as well, C2 and C3 already
will cover a case when previous leader is partitioned, so `{A, C} {B}` will not add anything new.

There obviously should be a way to set a limit, without one test generator will run
forever. In paxos TLA spec the meaningful limiting factor is a ballot number, however
the difference between TLA spec and current test generator is the absence of the global
messages set (each replica has its own mailbox), and therefore it is not possible
to decide if action should be made or not by checking this array.

The property that we will be testing for paxos is consistency, and in can expressed as
- if any replica learned a particular value any other replica can't learn another value.

#### Test generator

For the start i will define max number of steps that test generator can make.
Test is terminated after that number of steps or if every node doesn't have an action.

During test generations replicas are treated symmetric as well.
Test matrix without connectivity models looks like this (see actions for P1/P2 shortcuts):

|Replica/Action     | P1  |  P2 |
| ---               | --- | --- |
| A                 |     |     |
| B                 |     |     |
| C                 |     |     |

From the test table we need to draw every possible state at every step.
With symmetry list of states will be as short as:
- A P1 , B P1 , C P1
- A P2 , B P2 , C P2
- A P1 , B P2 , C P2
- A P1 , B P1 , C P2
Without symmetry it will need to be extended futher:
- A P1 , B P2 , C P1
- A P2 , B P1 , C P1
- A P2 , B P1 , C P2
- A P2,  B P2 , C P1

To generalize, if
