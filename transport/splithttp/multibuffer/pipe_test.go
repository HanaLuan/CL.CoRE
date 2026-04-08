package multibuffer

import (
	"errors"
	"io"
	"testing"
	"time"
)

func TestPipeReadSplit(t *testing.T) {
	pipe := New(32)
	if _, err := pipe.Write([]byte("abcdef")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	first, err := pipe.ReadChunk(4)
	if err != nil {
		t.Fatalf("first Read failed: %v", err)
	}
	if string(first) != "abcd" {
		t.Fatalf("first Read = %q, want %q", string(first), "abcd")
	}

	second, err := pipe.ReadChunk(4)
	if err != nil {
		t.Fatalf("second Read failed: %v", err)
	}
	if string(second) != "ef" {
		t.Fatalf("second Read = %q, want %q", string(second), "ef")
	}
}

func TestPipeInterrupt(t *testing.T) {
	pipe := New(32)
	wantErr := errors.New("upload failed")
	done := make(chan error, 1)

	go func() {
		_, err := pipe.ReadChunk(4)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	pipe.Interrupt(wantErr)

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Read error = %v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for interrupted Read")
	}
}

func TestPipeCloseEOF(t *testing.T) {
	pipe := New(32)
	done := make(chan error, 1)

	go func() {
		_, err := pipe.ReadChunk(4)
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	_ = pipe.Close()

	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("Read error = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed Read")
	}
}

func TestPipeBackpressure(t *testing.T) {
	pipe := New(4)
	if _, err := pipe.Write([]byte("abcd")); err != nil {
		t.Fatalf("initial Write failed: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := pipe.Write([]byte("ef"))
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("Write returned early with %v; expected blocking backpressure", err)
	case <-time.After(50 * time.Millisecond):
	}

	chunk, err := pipe.ReadChunk(4)
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}
	if string(chunk) != "abcd" {
		t.Fatalf("ReadChunk = %q, want %q", string(chunk), "abcd")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked Write failed after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked Write to resume")
	}
}

func TestPipeWriteAfterInterrupt(t *testing.T) {
	pipe := New(8)
	wantErr := errors.New("interrupted")
	pipe.Interrupt(wantErr)

	if _, err := pipe.Write([]byte("abc")); !errors.Is(err, wantErr) {
		t.Fatalf("Write error = %v, want %v", err, wantErr)
	}
}

func TestPipeLargeWriteRespectsBoundedCapacity(t *testing.T) {
	pipe := New(4)
	done := make(chan struct{})
	writeErr := make(chan error, 1)

	go func() {
		defer close(done)
		_, err := pipe.Write([]byte("abcdef"))
		writeErr <- err
	}()

	select {
	case <-done:
		t.Fatal("large Write returned before capacity was drained")
	case <-time.After(50 * time.Millisecond):
	}

	first, err := pipe.ReadChunk(4)
	if err != nil {
		t.Fatalf("first ReadChunk failed: %v", err)
	}
	if string(first) != "abcd" {
		t.Fatalf("first ReadChunk = %q, want %q", string(first), "abcd")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked large Write to resume")
	}

	if err := <-writeErr; err != nil {
		t.Fatalf("large Write failed: %v", err)
	}

	second, err := pipe.ReadChunk(4)
	if err != nil {
		t.Fatalf("second ReadChunk failed: %v", err)
	}
	if string(second) != "ef" {
		t.Fatalf("second ReadChunk = %q, want %q", string(second), "ef")
	}
}
