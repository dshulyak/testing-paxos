package paxos

import (
	"flag"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	workers = flag.Int("workers", runtime.NumCPU(), "number of workers that will run test cases")
	replay  = flag.String("replay", "", "replay test cases from the file")
	dir     = flag.String("dir", "", "directory for replay files. current workdir by default")
)

func makePath(name string) string {
	return filepath.Join(*dir, name)
}

type Runner func(*TestCase) error

func Run(t testing.TB, run Runner, opts ...GenOption) {
	type (
		tcErr struct {
			error
			tc *TestCase
		}
	)

	var (
		r struct {
			mu       sync.Mutex
			existing bool
			replay   *Replay
		}

		path = makePath(fmt.Sprintf("%s-%d.test", t.Name(), time.Now().UnixNano()))

		workers = *workers
		queue   = make(chan *TestCase, workers)

		errc = make(chan *tcErr, workers)
		wg   sync.WaitGroup
	)

	if len(*replay) > 0 {
		rpl, err := NewReplayReader(*replay)
		require.NoError(t, err)
		opts = append(opts, WithReplay(rpl))
		path = *replay
		r.existing = true
		r.replay = rpl
	}

	gen, err := NewGen(opts...)
	require.NoError(t, err)

	onError := func(tcerr *tcErr) {
		if !assert.NoError(t, tcerr, tcerr.tc.String()) {
			if r.existing {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()
			if r.replay == nil {
				replay, err := NewReplay(path)
				require.NoError(t, err, "can't create a replay file")
				r.replay = replay
			}
			require.NoError(t, r.replay.Write(tcerr.tc), "can't write to a replay file")
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tc := range queue {
				err := run(tc)
				if err != nil {
					// make sure to send at most one error from each worker
					// otherwise there will be a deadlock
					errc <- &tcErr{error: err, tc: tc}
					return
				}
			}
		}()
	}

	var (
		tcerr  *tcErr
		failed bool
	)
	for tc := gen.Next(); tc != nil && tcerr == nil; tc = gen.Next() {
		select {
		case queue <- tc:
		case tcerr = <-errc:
		}
		if tcerr != nil {
			failed = true
			onError(tcerr)
		}
	}

	close(queue)
	wg.Wait()
	close(errc)
	if !failed {
		for tcerr = range errc {
			if tcerr != nil && !failed {
				failed = true
				onError(tcerr)
			}
		}
	}

	require.NoError(t, gen.Error(), "internal generator error")
	if failed {
		require.NoError(t, r.replay.Close(), "can't close a replay file")
		t.Logf("Replay a failed test with: go test -run=%s -replay=%s",
			t.Name(), r.replay.Name(),
		)
	}
}
