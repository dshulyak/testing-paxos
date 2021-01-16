package paxos

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

type consistencyCheck struct {
	nodes    map[int]*Paxos
	messages []Message

	proposeTimeout int
	proposeTimers  map[int]int
}

func (c *consistencyCheck) Init(t *rapid.T) {
	size := rapid.IntRange(3, 3).Draw(t, "size").(int)
	c.nodes = make(map[int]*Paxos, size)
	nodes := make([]int, size)
	for id := 1; id <= size; id++ {
		nodes[id-1] = id
		c.nodes[id] = &Paxos{ID: id, Nodes: nodes}
	}
	c.proposeTimeout = 10
	c.proposeTimers = map[int]int{}
}

func (c *consistencyCheck) Propose(t *rapid.T) {
	id := rapid.IntRange(1, len(c.nodes)).Draw(t, "node").(int)

	timer := c.proposeTimers[id]
	if timer > 0 {
		c.proposeTimers[id]--
		t.Skipf("node %d cant yet propose", id)
	}
	c.proposeTimers[id] = c.proposeTimeout

	if c.nodes[id].LearnedValue != nil {
		t.Skipf("node %d already learned a value", id)
	}
	value := rapid.ArrayOf(3, rapid.Byte()).Draw(t, "value").([3]byte)
	c.nodes[id].Propose(Value(value[:]))
	for _, msg := range c.nodes[id].Messages {
		t.Logf("sending a messages %v", msg)
		c.messages = append(c.messages, msg)
	}
	c.nodes[id].Messages = nil
}

func (c *consistencyCheck) Send(t *rapid.T) {
	if len(c.messages) == 0 {
		t.Skip("no messages in the queue")
	}

	msg := c.messages[0]
	node := c.nodes[msg.To]

	node.Next(msg)

	t.Logf("delivered a message %v", msg)
	copy(c.messages, c.messages[1:])
	c.messages = c.messages[:len(c.messages)-1]
	for _, msg := range node.Messages {
		t.Logf("sending a messages %v", msg)
		c.messages = append(c.messages, msg)
	}
	node.Messages = nil
}

func (c *consistencyCheck) Check(t *rapid.T) {
	var value Value
	for _, n := range c.nodes {
		if n.LearnedValue != nil {
			if value != nil {
				require.Equal(t, value, n.LearnedValue, "Node=%v", n.ID)
			}
			value = n.LearnedValue
		}
	}
}

func TestConsistency(t *testing.T) {
	rapid.Check(t, rapid.Run(new(consistencyCheck)))
}
