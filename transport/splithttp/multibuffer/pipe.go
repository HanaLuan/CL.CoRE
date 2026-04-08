package multibuffer

import (
	"io"
	"sync"
)

const DefaultChunkSize = 8 * 1024

type Pipeline interface {
	Write([]byte) (int, error)
	ReadChunk(maxBytes int) ([]byte, error)
	Close() error
	Interrupt(error)
}

type chunk struct {
	data []byte
	off  int
}

type Pipe struct {
	mu        sync.Mutex
	cond      *sync.Cond
	queue     []chunk
	buffered  int
	limit     int
	chunkSize int
	closed    bool
	err       error
}

func New(limit int) *Pipe {
	p := &Pipe{
		limit:     limit,
		chunkSize: DefaultChunkSize,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *Pipe) Write(b []byte) (int, error) {
	written := 0
	for written < len(b) {
		p.mu.Lock()
		for {
			if p.err != nil {
				p.mu.Unlock()
				if written > 0 {
					return written, p.err
				}
				return 0, p.err
			}
			if p.closed {
				p.mu.Unlock()
				if written > 0 {
					return written, io.ErrClosedPipe
				}
				return 0, io.ErrClosedPipe
			}
			if p.limit <= 0 || p.buffered < p.limit {
				break
			}
			p.cond.Wait()
		}

		available := len(b) - written
		if p.limit > 0 {
			remaining := p.limit - p.buffered
			if remaining < available {
				available = remaining
			}
		}
		if available > p.chunkSize {
			available = p.chunkSize
		}
		if available <= 0 {
			p.mu.Unlock()
			continue
		}

		buf := make([]byte, available)
		copy(buf, b[written:written+available])
		p.queue = append(p.queue, chunk{data: buf})
		p.buffered += available
		written += available
		p.cond.Signal()
		p.mu.Unlock()
	}
	return written, nil
}

func (p *Pipe) ReadChunk(maxBytes int) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		if len(p.queue) > 0 {
			break
		}
		if p.err != nil {
			return nil, p.err
		}
		if p.closed {
			return nil, io.EOF
		}
		p.cond.Wait()
	}

	if maxBytes <= 0 {
		maxBytes = p.buffered
	}

	outLen := minInt(maxBytes, p.buffered)
	out := make([]byte, outLen)
	read := 0
	for len(p.queue) > 0 && read < outLen {
		head := &p.queue[0]
		n := copy(out[read:], head.data[head.off:])
		head.off += n
		read += n
		p.buffered -= n
		if head.off == len(head.data) {
			p.queue[0] = chunk{}
			p.queue = p.queue[1:]
		}
	}
	p.cond.Broadcast()
	return out, nil
}

func (p *Pipe) Interrupt(err error) {
	p.mu.Lock()
	if err == nil {
		err = io.ErrClosedPipe
	}
	if p.err == nil {
		p.err = err
	}
	for i := range p.queue {
		p.queue[i] = chunk{}
	}
	p.queue = nil
	p.buffered = 0
	p.cond.Broadcast()
	p.mu.Unlock()
}

func (p *Pipe) Close() error {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
	return nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
