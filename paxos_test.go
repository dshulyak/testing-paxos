package paxos

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPaxos(t *testing.T) {
	n := 3
	nodes := []Paxos{}
	replicas := make([]int, n)
	for i := 1; i <= n; i++ {
		replicas[i-1] = i
		nodes = append(nodes, Paxos{ID: i, Nodes: replicas})
	}

	gen, err := NewGen(
		WithExplicitPartitions(
			[][]int{
				{1, 2},
				{3},
			},
			[][]int{
				{1},
				{2, 3},
			},
		),
		WithReplicas(replicas...),
		WithLeaders(1, 3),
		WithSteps(9),
	)

	require.NoError(t, err)

	runTestCase := func(tc *TestCase) error {
		// per step message queue
		messages := []Message{}
		delayed := []Message{}

		cluster := make(map[int]*Paxos, n)
		for i := range nodes {
			node := nodes[i]
			cluster[node.ID] = &node
		}

		for {
			network, actions := tc.Next()
			if network == nil || actions == nil {
				return nil
			}

			for _, node := range cluster {
				if actions.IsLeader(node.ID) {
					node.Propose([]byte{byte(node.ID)})
				}
				messages = append(messages, node.Messages...)
				node.Messages = node.Messages[:0]
			}

			for _, msg := range messages {
				// messages that can't reach other node are delayed not dropped
				if network.Reachable(msg.From, msg.To) {
					cluster[msg.To].Next(msg)
				} else {
					delayed = append(delayed, msg)
				}
			}
			messages = delayed
			delayed = delayed[:0]

			var learned Value
			for _, node := range cluster {
				if node.LearnedValue != nil && learned == nil {
					learned = node.LearnedValue
				} else if node.LearnedValue != nil {
					if bytes.Compare(learned, node.LearnedValue) != 0 {
						return fmt.Errorf("%v != %v", learned, node.LearnedValue)
					}
				}
			}
		}
	}

	var (
		workers = runtime.NumCPU()
		queue   = make(chan *TestCase, workers)
		errc    = make(chan error, workers)
		wg      sync.WaitGroup
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tc := range queue {
				err := runTestCase(tc)
				if err != nil {
					errc <- err
					return
				}
			}
		}()
	}

	for tc := gen.Next(); tc != nil && err == nil; tc = gen.Next() {
		select {
		case queue <- tc:
		case err = <-errc:
			assert.NoError(t, err)
		}
	}

	close(queue)
	wg.Wait()
	close(errc)
	for err := range errc {
		assert.NoError(t, err)
	}
	t.Logf("Test cases: %d", gen.Count())
}
