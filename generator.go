package paxos

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
)

const (
	defaultStepLimit = 10
)

type GenOption func(g *Generator) error

// each item in the list describes separate state of the network.
// state of the network is expressed as list with list of nodes that can reach
// each other
// example:
// - - []int{ 1, 2 }
//   - []int{ 3 }
// - - []int{ 1 }
//   - []int{ 2 }
//   - []int{ 3 }
// - - []int{ 1, 2, 3 }
func WithExplicitPartitions(networks ...[][]int) GenOption {
	return func(g *Generator) error {
		for _, network := range networks {
			partition := Partition{}
			for _, nodes := range network {
				for i := 0; i < len(nodes)-1; i++ {
					for _, to := range nodes[i+1:] {
						partition.Add(nodes[i], to)
					}
				}
			}
			g.partitions = append(g.partitions, partition)
		}
		return nil
	}
}

func WithReplicas(replicas ...int) GenOption {
	return func(g *Generator) error {
		g.replicas = replicas
		return nil
	}
}

func WithSteps(stepLimit int) GenOption {
	return func(g *Generator) error {
		g.stepLimit = stepLimit
		return nil
	}
}

// WithLeaders specifies which nodes may be selected as a leader and generates approprite action state.
// Always generates an action without leaders
//
// Generator needs to provide all possible states of the cluster at a given step.
// Manually limiting leader down to 2 (for example) will significantly reduce scope of the test.
func WithLeaders(leaders ...int) GenOption {
	return func(g *Generator) error {
		if g.replicas == nil {
			return fmt.Errorf("replicas must be configured earlier than leaders")
		}
		g.actions = append(g.actions, Actions{})
		for _, leader := range leaders {
			g.actions = append(g.actions, Actions{leader: true})
		}
		return nil
	}
}

func NewGen(opts ...GenOption) (*Generator, error) {
	gen := &Generator{}
	for _, opt := range opts {
		if err := opt(gen); err != nil {
			return nil, err
		}
	}
	if gen.stepLimit == 0 {
		gen.stepLimit = defaultStepLimit
	}
	if gen.actions == nil {
		return nil, errors.New("provide an option to configure actions")
	}
	if gen.partitions == nil {
		return nil, errors.New("provide an option to configure partitions")
	}

	for i := range gen.actions {
		for j := range gen.partitions {
			gen.states = append(gen.states, stepState{actions: i, partition: j})
		}
	}
	gen.cnts = make([]int, gen.stepLimit)
	return gen, nil
}

type stepState struct {
	actions, partition int
}

func (s stepState) String() string {
	return fmt.Sprintf("(a=%d p=%d)", s.actions, s.partition)
}

type Generator struct {
	mu    sync.Mutex
	ended bool
	// permutation counters
	cnts []int

	// total number of generated test cases
	cnt int

	stepLimit int
	// permutation of actions and partitions
	states []stepState

	replicas   []int
	partitions []Partition
	actions    []Actions
}

// Next is used to generate test cases. Nil test case - generator was exhaused.
// Safe to use from multiple goroutines.
func (g *Generator) Next() *TestCase {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.ended {
		return nil
	}

	// generates a product of all states repeating it stepLimit times
	// similar to python's itertool.product but stack is kept on the generator object.
	steps := make([]int, g.stepLimit)
	copy(steps, g.cnts)

	for i := len(g.cnts) - 1; i >= 0; i-- {
		g.cnts[i]++
		if g.cnts[i] < len(g.states) {
			break
		}
		g.cnts[i] = 0
		if i == 0 {
			g.ended = true
		}
	}
	g.cnt++
	return &TestCase{gen: g, states: steps}
}

// Count returns total number of generated test cases.
func (gen *Generator) Count() int {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	return gen.cnt
}

type TestCase struct {
	gen *Generator

	states []int
	step   int
}

func (t *TestCase) Next() (Partition, Actions) {
	if t.step == len(t.states) {
		return nil, nil
	}

	state := t.gen.states[t.states[t.step]]
	t.step++

	return t.gen.partitions[state.partition], t.gen.actions[state.actions]
}

type Partition map[int]map[int]struct{}

func (p Partition) Add(from, to int) {
	p.add(from, to)
	p.add(to, from)
}

func (p Partition) add(from, to int) {
	routes, ok := p[from]
	if !ok {
		routes = map[int]struct{}{}
		p[from] = routes
	}
	routes[to] = struct{}{}
}

// Reachable returns true if route is not blocked.
func (p Partition) Reachable(from, to int) bool {
	routes, ok := p[from]
	if !ok {
		return false
	}
	_, ok = routes[to]
	return ok
}

func (p Partition) String() string {
	var b bytes.Buffer
	b.WriteString("Routes(")
	var first = true
	for from, dest := range p {
		if first {
			first = false
		} else {
			b.WriteString(",")
		}
		for to := range dest {
			fmt.Fprintf(&b, "%d=>%d", from, to)
		}
	}
	b.WriteString(")")
	return b.String()
}

type Actions map[int]bool

func (a Actions) IsLeader(replica int) bool {
	return a[replica]
}
