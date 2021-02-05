package paxos

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

const (
	crcWidth    = 4
	lengthWidth = 4
	metaWidth   = 8
)

type flusher interface {
	Flush() error
}

func NewReplay(path string) (*Replay, error) {
	r, err := openReplay(path, os.O_CREATE|os.O_WRONLY)
	if err != nil {
		return nil, err
	}
	wr := bufio.NewWriter(r.f)
	r.writer = wr
	r.flush = wr
	return r, nil
}

func NewReplayReader(path string) (*Replay, error) {
	r, err := openReplay(path, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	r.reader = bufio.NewReader(r.f)
	return r, nil
}

func openReplay(path string, flag int) (*Replay, error) {
	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return nil, err
	}
	return &Replay{f: f}, nil
}

type Replay struct {
	mu sync.Mutex
	f  *os.File

	metaBuf [metaWidth]byte

	writer io.Writer
	flush  flusher

	reader io.Reader
}

func (r *Replay) Name() string {
	return r.f.Name()
}

func (r *Replay) Write(tc *TestCase) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	buf, err := tc.Marshal()
	if err != nil {
		return err
	}
	code := crc32.Update(0, crcTable, buf)
	lth := uint32(len(buf))

	binary.LittleEndian.PutUint32(r.metaBuf[:lengthWidth], lth)
	binary.LittleEndian.PutUint32(r.metaBuf[lengthWidth:metaWidth], code)

	sum := 0
	n, err := r.writer.Write(r.metaBuf[:])
	if err != nil {
		return err
	}
	sum += n
	n, err = r.writer.Write(buf)
	if err != nil {
		return err
	}
	sum += n

	if sum != metaWidth+len(buf) {
		return errors.New("can't write meta and payload buffers")
	}
	return nil
}

func (r *Replay) Read() (*TestCase, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, err := io.ReadFull(r.reader, r.metaBuf[:])
	if err != nil {
		return nil, err
	}

	lth := binary.LittleEndian.Uint32(r.metaBuf[:lengthWidth])
	code := binary.LittleEndian.Uint32(r.metaBuf[lengthWidth:metaWidth])

	buf := make([]byte, lth)
	_, err = io.ReadFull(r.reader, buf)
	if err != nil {
		return nil, err
	}

	rcode := crc32.Update(0, crcTable, buf)
	if rcode != code {
		return nil, errors.New("replay file is corrupted")
	}

	var tc TestCase
	if err := tc.Unmarshal(buf); err != nil {
		return nil, err
	}
	return &tc, nil
}

func (r *Replay) Close() error {
	if r.flush != nil {
		if err := r.flush.Flush(); err != nil {
			return err
		}
	}
	return r.f.Close()
}
