package migrations

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/watchedsky-social/libwatchedsky"
)

type migrationContextKey struct{}

var sdrKey migrationContextKey

var ErrInvalidSourceDataRoot = errors.New("invalid source data root dir")

// SetSourceDataRoot sets the directory on the current context and returns a new
// [context.Context] that can be passed to [SourceDataRoot]
func SetSourceDataRoot(ctx context.Context, rootDir string) (context.Context, error) {
	if ctx == nil {
		return nil, libwatchedsky.ErrNilContext
	}

	absRoot, err := validateSourceRoot(rootDir)
	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, sdrKey, absRoot), nil
}

// SourceDataRoot returns the directory where source data exists, or an error if the context is nil,
// the value is not set, or not a directory, or cannot be coerced into an absolute path
func SourceDataRoot(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", libwatchedsky.ErrNilContext
	}

	dir, ok := ctx.Value(sdrKey).(string)
	if !ok {
		return "", fmt.Errorf("%w: directory not set", ErrInvalidSourceDataRoot)
	}

	return validateSourceRoot(dir)
}

func validateSourceRoot(dir string) (string, error) {
	s, err := os.Stat(dir)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidSourceDataRoot, err)
	}

	if !s.IsDir() {
		return "", fmt.Errorf("%w: %s is not a directory", ErrInvalidSourceDataRoot, dir)
	}

	return filepath.Abs(dir)
}
