package splithttp

import (
	"errors"
	"io"
	"sync"
)

var ErrQueueTooLarge = errors.New("packet queue is too large")

type Packet struct {
	Seq     uint64
	Payload []byte
	Reader  io.ReadCloser
}

type uploadQueue struct {
	mu         sync.Mutex
	condPushed sync.Cond
	condPopped sync.Cond
	packets    map[uint64][]byte
	nextSeq    uint64
	buf        []byte
	closed     bool
	maxPackets int
	reader     io.ReadCloser
}

func NewUploadQueue(maxPackets int) *uploadQueue {
	q := &uploadQueue{
		packets:    make(map[uint64][]byte, maxPackets),
		maxPackets: maxPackets,
	}
	q.condPushed = sync.Cond{L: &q.mu}
	q.condPopped = sync.Cond{L: &q.mu}
	return q
}

func (q *uploadQueue) Push(p Packet) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return io.ErrClosedPipe
	}
	if q.reader != nil {
		return errors.New("uploadQueue.reader already exists")
	}
	if p.Reader != nil {
		q.reader = p.Reader
		q.condPushed.Broadcast()
		return nil
	}

	for len(q.packets) > q.maxPackets {
		q.condPopped.Wait()
		if q.closed {
			return io.ErrClosedPipe
		}
	}

	q.packets[p.Seq] = p.Payload
	q.condPushed.Broadcast()
	return nil
}

func (q *uploadQueue) Read(b []byte) (int, error) {
	q.mu.Lock()

	for {
		if len(q.buf) > 0 {
			n := copy(b, q.buf)
			q.buf = q.buf[n:]
			q.mu.Unlock()
			return n, nil
		}

		if payload, ok := q.packets[q.nextSeq]; ok {
			delete(q.packets, q.nextSeq)
			q.nextSeq++
			q.buf = payload
			q.condPopped.Broadcast()
			continue
		}

		if reader := q.reader; reader != nil {
			q.mu.Unlock()
			return reader.Read(b)
		}

		if q.closed {
			q.mu.Unlock()
			return 0, io.EOF
		}

		if len(q.packets) > q.maxPackets {
			q.mu.Unlock()
			return 0, ErrQueueTooLarge
		}

		q.condPushed.Wait()
	}
}

func (q *uploadQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	var err error
	if q.reader != nil {
		err = q.reader.Close()
	}
	q.closed = true
	q.condPushed.Broadcast()
	q.condPopped.Broadcast()
	return err
}
