package paxos

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
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
		g.nodes = replicas
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
		if g.nodes == nil {
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
	if len(gen.states) > math.MaxInt16 {
		return nil, fmt.Errorf("max number of possible states %d. reduce by removing actions or partitions", math.MaxInt16)
	}
	if gen.iter == nil {
		gen.iter = &productIterator{gen: gen, cnts: make([]int16, gen.stepLimit)}
	}
	return gen, nil
}

type stepState struct {
	actions, partition int
}

func (s stepState) String() string {
	return fmt.Sprintf("(a=%d p=%d)", s.actions, s.partition)
}

type Generator struct {
	mu   sync.Mutex
	iter tcIterator

	// total number of generated test cases
	cnt int

	stepLimit int
	// permutation of actions and partitions
	states []stepState

	nodes      []int
	partitions []Partition
	actions    []Actions
}

// Next is used to generate test cases. Nil test case - generator was exhaused.
// Safe to use from multiple goroutines.
func (g *Generator) Next() *TestCase {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.iter.Next() {
		return nil
	}
	g.cnt++
	return g.iter.Current()
}

func (g *Generator) Error() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.iter.Error()
}

// Count returns total number of generated test cases.
func (gen *Generator) Count() int {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	return gen.cnt
}

type TestCase struct {
	gen *Generator

	states []int16
	step   int
}

func (t *TestCase) Nodes() []int {
	return t.gen.nodes
}

func (t *TestCase) Next() (Partition, Actions) {
	if t.step == len(t.states) {
		return nil, nil
	}

	state := t.gen.states[t.states[t.step]]
	t.step++

	return t.gen.partitions[state.partition], t.gen.actions[state.actions]
}

func (t *TestCase) String() string {
	var buf bytes.Buffer
	for i := 0; i <= t.step && i < len(t.states); i++ {
		state := t.gen.states[t.states[i]]
		fmt.Fprintf(&buf, "step %d: %s %s\n", i+1,
			t.gen.partitions[state.partition],
			t.gen.actions[state.actions],
		)
	}
	return buf.String()
}

func (t *TestCase) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, int64(len(t.states))); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, t.states); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (t *TestCase) Unmarshal(b []byte) error {
	buf := bytes.NewBuffer(b)
	var lth int64
	if err := binary.Read(buf, binary.LittleEndian, &lth); err != nil {
		return err
	}
	t.states = make([]int16, lth)
	if err := binary.Read(buf, binary.LittleEndian, t.states); err != nil {
		return err
	}
	return nil
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
	for from, dest := range p {
		for to := range dest {
			fmt.Fprintf(&b, "%d=>%d,", from, to)
		}
	}
	b.WriteString(")")
	return b.String()
}

type Actions map[int]bool

func (a Actions) IsLeader(replica int) bool {
	return a[replica]
}

func (a Actions) String() string {
	var buf bytes.Buffer
	buf.WriteString("Cluster(")
	for id := range a {
		fmt.Fprintf(&buf, "leader=%d", id)
	}
	buf.WriteString(")")
	return buf.String()
}

type tcIterator interface {
	Next() bool
	Error() error
	Current() *TestCase
}

type productIterator struct {
	gen *Generator

	ended bool
	// permutation counters
	cnts    []int16
	current *TestCase
}

func (pi *productIterator) Next() bool {
	if pi.ended {
		return false
	}

	states := make([]int16, pi.gen.stepLimit)
	copy(states, pi.cnts)

	for i := len(pi.cnts) - 1; i >= 0; i-- {
		pi.cnts[i]++
		if pi.cnts[i] < int16(len(pi.gen.states)) {
			break
		}
		pi.cnts[i] = 0
		if i == 0 {
			pi.ended = true
		}
	}

	pi.current = &TestCase{gen: pi.gen, states: states}
	return true
}

func (pi *productIterator) Current() *TestCase {
	return pi.current
}

func (pi *productIterator) Error() error {
	return nil
}

func WithReplay(r *Replay) GenOption {
	return func(gen *Generator) error {
		gen.iter = &replayIterator{gen: gen, r: r}
		return nil
	}
}

type replayIterator struct {
	gen *Generator

	r       *Replay
	err     error
	current *TestCase
}

func (r *replayIterator) Next() bool {
	if r.err != nil {
		return false
	}
	r.current, r.err = r.r.Read()
	if r.current != nil {
		r.current.gen = r.gen
	}
	return r.err == nil
}

func (r *replayIterator) Current() *TestCase {
	return r.current
}

func (r *replayIterator) Error() error {
	if r.err == nil || errors.Is(r.err, io.EOF) {
		return nil
	}
	return r.err
}
