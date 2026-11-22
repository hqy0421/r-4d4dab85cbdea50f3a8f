package internal

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// failingSink wraps a Sink and injects failures at the exact durability
// boundaries the recovery protocol cares about:
//
//   - failManifestOnce: the first WriteManifest after at least one delta has
//     been uploaded returns an error ("files landed, commit lost").
//   - hidePaths: ReadFile on these remote paths fails to simulate an upload
//     that never completed or an object lost before manifest commit.
type failingSink struct {
	ports.Sink

	mu sync.Mutex

	manifestFailsLeft int
	manifestAttempts  int
	deltaUploads      int
	hidePaths         map[string]bool
}

func newFailingSink(inner ports.Sink) *failingSink {
	return &failingSink{Sink: inner, hidePaths: map[string]bool{}}
}

func (s *failingSink) WriteFile(ctx context.Context, remotePath string, r io.Reader) error {
	if err := s.Sink.WriteFile(ctx, remotePath, r); err != nil {
		return err
	}
	s.mu.Lock()
	s.deltaUploads++
	// A successful (re)upload makes the object readable again — hiding is a
	// pre-replay state, not a permanent fault.
	delete(s.hidePaths, remotePath)
	s.mu.Unlock()
	return nil
}

func (s *failingSink) WriteManifest(ctx context.Context, m *manifest.Manifest) error {
	s.mu.Lock()
	s.manifestAttempts++
	if s.manifestFailsLeft > 0 {
		s.manifestFailsLeft--
		s.mu.Unlock()
		return fmt.Errorf("injected manifest failure (attempt %d)", s.manifestAttempts)
	}
	s.mu.Unlock()
	return s.Sink.WriteManifest(ctx, m)
}

func (s *failingSink) ReadFile(ctx context.Context, path string) (io.ReadCloser, error) {
	s.mu.Lock()
	hidden := s.hidePaths[path]
	s.mu.Unlock()
	if hidden {
		return nil, fmt.Errorf("injected: %s not found", path)
	}
	return s.Sink.ReadFile(ctx, path)
}

func (s *failingSink) stats() (attempts, uploads int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.manifestAttempts, s.deltaUploads
}
