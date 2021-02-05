package paxos

import (
	"flag"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	workers = flag.Int("workers", runtime.NumCPU(), "number of workers that will run test cases")
	replay  = flag.String("replay", "", "replay test cases from the file")
)

type Runner func(*TestCase) error

func Run(t testing.TB, run Runner, opts ...GenOption) {
	type (
		tcErr struct {
			error
			tc *TestCase
		}
	)

	var (
		workers  = *workers
		queue    = make(chan *TestCase, workers)
		tcerr    *tcErr
		errc     = make(chan *tcErr, workers)
		wg       sync.WaitGroup
		gen, err = NewGen(opts...)
	)
	require.NoError(t, err)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tc := range queue {
				err := run(tc)
				if err != nil {
					// make sure to send at most one error from each worker
					// otherwise there is a deadlock situtation
					errc <- &tcErr{error: err, tc: tc}
					return
				}
			}
		}()
	}

	for tc := gen.Next(); tc != nil && err == nil; tc = gen.Next() {
		select {
		case queue <- tc:
		case tcerr = <-errc:
		}
		if tcerr != nil {
			if !assert.NoError(t, tcerr, tcerr.tc.String()) {
				break
			}
		}
	}

	close(queue)
	wg.Wait()
}
