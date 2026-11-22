package internal

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseLSN converts a PostgreSQL LSN string ("0/16B3740") into its 64-bit
// byte offset. The core keeps this independent of pglogrepl so the hex
// architecture boundary stays inside the postgres adapter.
func ParseLSN(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty LSN")
	}
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN %q: expected X/YYYYYYY", s)
	}
	hi, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid LSN %q: %w", s, err)
	}
	lo, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid LSN %q: %w", s, err)
	}
	return (hi << 32) | lo, nil
}

// CompareLSN returns -1, 0, or 1 comparing two LSN strings by byte offset.
// An empty LSN sorts before every real LSN; equal-or-unparseable inputs are
// treated conservatively (unparseable values return an error to the caller).
func CompareLSN(a, b string) (int, error) {
	if a == "" && b == "" {
		return 0, nil
	}
	if a == "" {
		return -1, nil
	}
	if b == "" {
		return 1, nil
	}
	va, err := ParseLSN(a)
	if err != nil {
		return 0, err
	}
	vb, err := ParseLSN(b)
	if err != nil {
		return 0, err
	}
	switch {
	case va < vb:
		return -1, nil
	case va > vb:
		return 1, nil
	default:
		return 0, nil
	}
}

// maxLSN returns the LSN string with the greater byte offset. Empty inputs
// are ignored; it returns "" only when both inputs are empty.
func maxLSN(a, b string) (string, error) {
	cmp, err := CompareLSN(a, b)
	if err != nil {
		return "", err
	}
	if cmp >= 0 {
		return a, nil
	}
	return b, nil
}
