package paxos

import (
	"math/rand"
)

type randomIterator struct {
	// any number between 0 and 100. where 100 means execute every test case
	percent int
	rng     *rand.Rand
	iter    tcIterator
}

func (r *randomIterator) Next() bool {
	for {
		next := r.iter.Next()
		if !next || r.rng.Intn(100) <= r.percent {
			return next
		}
	}
}

func (r *randomIterator) Error() error {
	return r.iter.Error()
}

func (r *randomIterator) Current() *TestCase {
	return r.iter.Current()
}
