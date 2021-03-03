package paxos

import "fmt"

type MessageType int8

const (
	MessageEmpty MessageType = iota
	MessagePrepare
	MessagePromise
	MessageAccept
	MessageAccepted
)

var messageTypeString = [...]string{
	"Empty",
	"Prepare",
	"Promise",
	"Accept",
	"Accepted",
}

func (m MessageType) String() string {
	return messageTypeString[m]
}

type Value []byte

func (v Value) String() string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("0x%x", string(v))
}

type Message struct {
	From, To int

	Type MessageType

	Ballot int
	// non-null if:
	// Promise - previously selected value
	// Accept  - value to select in current ballot
	Value Value

	// non-null only if the Type is Promise
	VotedBallot int
}

func (m Message) String() string {
	return fmt.Sprintf("Msg[From=%d To=%d Ballot=%d Type=%s VBallot=%d Value=%s]",
		m.From, m.To, m.Ballot, m.Type, m.VotedBallot, m.Value,
	)
}

type Paxos struct {
	// unique node identifier
	ID    int
	Nodes []int

	// Round 1 and 2 majorities
	// For correctness it is not necessary to get exactly half + 1 responses
	// in every round, but it is necessary to get an intersection between two
	// majorities
	R1Majority, R2Majority int

	// current ballot. monotonically growing.
	// persisted in practice
	ballot int

	// tracking Promises
	promises map[int]struct{}
	// value from a Promise with a highest observed ballot.
	promiseValue Value
	// highest observed ballot from all received promises
	promiseBallot int

	// tracking Accepted
	accepts map[int]struct{}

	// selected value. persisted
	votedValue Value
	// ballot when the value was selected. persisted
	votedBallot int

	// value proposed by this node.
	value Value

	// value selected by the majority.
	// Algorithm is invalid if it is modified more to any other value after
	// it was updated the first time.
	LearnedValue Value

	// current replica outbox. only messages to other nodes.
	// state changes to the current replica are applied immediatly.
	Messages []Message
}

func (p *Paxos) Propose(value Value) {
	// Phase 1A.
	// Increment a ballot and send Prepare to every other Acceptor.
	p.value = value
	p.ballot++
	for _, id := range p.Nodes {
		if id != p.ID {
			p.Messages = append(p.Messages, Message{
				From:   p.ID,
				To:     id,
				Type:   MessagePrepare,
				Ballot: p.ballot,
			})
		}
	}
	p.promises = map[int]struct{}{}
	p.promiseBallot = 0
	p.promiseValue = nil
	p.updatePromise(p.ID, p.votedValue, p.votedBallot)

	p.accepts = map[int]struct{}{}
}

func (p *Paxos) updatePromise(id int, votedValue Value, votedBallot int) {
	p.promises[id] = struct{}{}
	if votedBallot > p.promiseBallot {
		p.promiseBallot = votedBallot
		p.promiseValue = votedValue
	}
}

func (p *Paxos) Next(m Message) {
	if m.Ballot < p.ballot {
		return
	}
	if m.To != p.ID {
		panic(fmt.Errorf("id mismatch. destination %d, received %d", m.To, p.ID))
	}
	switch m.Type {
	case MessagePrepare:
		// Phase 1A. If msg ballot is higher than the local ballot reply with Promise and save the ballot.
		if m.Ballot > p.ballot {
			p.ballot = m.Ballot
			p.Messages = append(p.Messages, Message{
				From:        p.ID,
				To:          m.From,
				Type:        MessagePromise,
				Ballot:      m.Ballot,
				VotedBallot: p.votedBallot,
				Value:       p.votedValue,
			})
		}
	case MessagePromise:
		// Phase 1B. Collect Promises from majority, chose non-null
		// promise with the highest observed voted ballot.
		// If there is no existing non-null promise propose locally chosen value.
		if m.Ballot == p.ballot {
			p.updatePromise(m.From, m.Value, m.VotedBallot)
			if len(p.promises) == p.R1Majority {
				if p.promiseValue == nil {
					p.promiseValue = p.value
				}
				for _, id := range p.Nodes {
					if id == p.ID {
						continue
					}
					p.Messages = append(p.Messages, Message{
						From:   p.ID,
						To:     id,
						Type:   MessageAccept,
						Ballot: p.ballot,
						Value:  p.promiseValue,
					})
				}
				// proposer gives a vote explicitly, without sending a message to itself
				p.votedValue = p.promiseValue
				p.votedBallot = p.ballot
				p.accepts[p.ID] = struct{}{}
			}
		}
	case MessageAccept:
		// Phase 2A. If Accept ballot is atleast as high as a local ballot
		// save proposed value and ballot and reply with Accepted.
		if m.Ballot >= p.ballot {
			p.ballot = m.Ballot
			p.votedValue = m.Value
			p.votedBallot = m.Ballot
			p.Messages = append(p.Messages, Message{
				From:   p.ID,
				To:     m.From,
				Type:   MessageAccepted,
				Ballot: m.Ballot,
			})
		}
	case MessageAccepted:
		// Phase 2B. Collect Accepted from majority, set Learned value to
		// previously chosen value.
		if m.Ballot == p.ballot {
			p.accepts[m.From] = struct{}{}
			if len(p.accepts) == p.R2Majority {
				p.LearnedValue = p.votedValue
			}
		}
	}
}
