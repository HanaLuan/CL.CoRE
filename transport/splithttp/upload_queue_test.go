package splithttp

import (
	"io"
	"testing"
	"time"
)

func TestUploadQueueDropsDuplicateWithoutBusyRead(t *testing.T) {
	queue := NewUploadQueue(4)
	if err := queue.Push(Packet{Payload: []byte("ab"), Seq: 0}); err != nil {
		t.Fatalf("push seq 0 failed: %v", err)
	}
	if err := queue.Push(Packet{Payload: []byte("ab"), Seq: 0}); err != nil {
		t.Fatalf("push duplicate seq 0 failed: %v", err)
	}

	buf := make([]byte, 2)
	n, err := queue.Read(buf)
	if err != nil {
		t.Fatalf("first read failed: %v", err)
	}
	if got := string(buf[:n]); got != "ab" {
		t.Fatalf("first read = %q, want %q", got, "ab")
	}

	done := make(chan struct {
		n   int
		err error
	}, 1)
	go func() {
		n, err := queue.Read(buf)
		done <- struct {
			n   int
			err error
		}{n: n, err: err}
	}()

	select {
	case res := <-done:
		t.Fatalf("duplicate-only read returned early: n=%d err=%v", res.n, res.err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := queue.Push(Packet{Payload: []byte("cd"), Seq: 1}); err != nil {
		t.Fatalf("push seq 1 failed: %v", err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("second read failed: %v", res.err)
		}
		if got := string(buf[:res.n]); got != "cd" {
			t.Fatalf("second read = %q, want %q", got, "cd")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-order packet")
	}
}

func TestUploadQueueRejectsOversizedReassemblyBuffer(t *testing.T) {
	queue := NewUploadQueue(1)
	if err := queue.Push(Packet{Payload: []byte("late"), Seq: 2}); err != nil {
		t.Fatalf("push seq 2 failed: %v", err)
	}
	if err := queue.Push(Packet{Payload: []byte("later"), Seq: 3}); err != nil {
		t.Fatalf("push seq 3 failed: %v", err)
	}

	buf := make([]byte, 8)
	_, err := queue.Read(buf)
	if err == nil || err.Error() != "closed" {
		t.Fatalf("Read error = %v, want closed", err)
	}

	if err := queue.Close(); err != nil && err != io.EOF {
		t.Fatalf("Close failed: %v", err)
	}
}
