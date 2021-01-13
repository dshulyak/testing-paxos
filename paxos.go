package paxos

import "fmt"

type MessageType uint8

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

type Message struct {
	From, To uint64

	Type MessageType

	Ballot uint64
	// non-null if:
	// Promise - previously selected value
	// Accept  - value to select in current ballot
	Value []byte

	// non-null only if the Type is Promise
	VotedBallot uint64
}

type Paxos struct {
	// unique node identifier
	ID    uint64
	Nodes []uint64

	// current ballot. monotonically growing.
	ballot uint64

	// tracking Promises
	promises map[uint64]struct{}
	// value from a Promise with a highest observed ballot.
	promiseValue []byte
	// highest observed ballot among Promise.VotedBallot
	promiseBallot uint64

	// tracking Accepted
	accepts map[uint64]struct{}

	// selected value
	votedValue []byte
	// ballot when the value was selected
	votedBallot uint64

	// value proposed by this node.
	value []byte

	// value selected by the majority.
	// Once set it can not be modified.
	LearnedValue []byte

	Messages []Message
}

func (p *Paxos) Propose(value []byte) {
	// Phase 1A.
	// Increment a ballot and send Prepare to every other Acceptor.
	p.value = value
	p.ballot++
	for _, id := range p.Nodes {
		if id != p.ID {
			p.Messages = append(p.Messages, Message{
				From:   p.ID,
				To:     p.ID,
				Type:   MessagePrepare,
				Ballot: p.ballot,
			})
		}
	}
	p.promises = map[uint64]struct{}{}
	p.promiseBallot = 0
	p.promiseValue = nil
	p.updatePromise(p.ID, p.votedValue, p.votedBallot)
	p.accepts = map[uint64]struct{}{}
}

func (p *Paxos) updatePromise(id uint64, votedValue []byte, votedBallot uint64) {
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
		panic(fmt.Errorf("id mismatch. destination %d, but was received by %d", m.To, p.ID))
	}
	switch m.Type {
	case MessagePrepare:
		// Phase 1A. Save Prepare ballot if it higher then the local
		// and reply with Promise, that includes previously voted ballot and value
		if m.Ballot > p.ballot {
			p.ballot = m.Ballot
			p.Messages = append(p.Messages, Message{
				From:        p.ID,
				To:          m.From,
				Type:        MessagePromise,
				Ballot:      p.ballot,
				VotedBallot: p.votedBallot,
				Value:       p.votedValue,
			})
		}
	case MessagePromise:
		// Phase 1B. Collect Promise's from majority, chose non-null
		// promise from the highest observed vote. If there is no such promise
		// chose locally proposed value.
		if m.Ballot == p.ballot {
			p.updatePromise(m.From, m.Value, m.VotedBallot)
			if len(p.promises) == (len(p.Nodes)+1)/2 {
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
				Ballot: p.ballot,
			})
		}
	case MessageAccepted:
		// Phase 2B. Collect Accepted from majority, set Learned value to
		// previously chosen value.
		if m.Ballot == p.ballot {
			p.accepts[m.From] = struct{}{}
			if len(p.accepts) == (len(p.Nodes)+1)/2 {
				if p.LearnedValue != nil {
					panic("consistency violation")
				}
				p.LearnedValue = p.promiseValue
			}
		}
	}
}
