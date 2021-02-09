package paxos

import (
	"bytes"
	"fmt"
	"testing"
)

func paxosRunner(tc *TestCase) error {
	messages := []Message{}
	delayed := []Message{}

	nodes := tc.Nodes()
	cluster := make(map[int]*Paxos, len(nodes))
	for _, id := range nodes {
		cluster[id] = &Paxos{ID: id, Nodes: nodes, R1Majority: 4, R2Majority: 2}
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

func TestPaxos(t *testing.T) {
	Run(t, paxosRunner,
		WithExplicitPartitions(
			[][]int{
				{1, 2, 3},
				{4, 5},
			},
			[][]int{
				{1, 2},
				{3, 4, 5},
			},
		),
		WithReplicas(1, 2, 3, 4, 5),
		WithLeaders(1, 3),
		WithSteps(9),
	)
}
