package internal

import "testing"

func TestParseLSN(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"0/0", 0},
		{"0/16B3740", 0x16B3740},
		{"1/16B3740", (uint64(1) << 32) | 0x16B3740},
		{"FFFFFFFF/FFFFFFFF", ^uint64(0)},
	}
	for _, tc := range cases {
		got, err := ParseLSN(tc.in)
		if err != nil {
			t.Fatalf("ParseLSN(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("ParseLSN(%q) = %#x, want %#x", tc.in, got, tc.want)
		}
	}
}

func TestParseLSNErrors(t *testing.T) {
	for _, in := range []string{"", "junk", "16B3740", "x/1", "1/zzz"} {
		if _, err := ParseLSN(in); err == nil {
			t.Errorf("ParseLSN(%q) expected error", in)
		}
	}
}

func TestCompareLSN(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"", "", 0},
		{"", "0/1", -1},
		{"0/1", "", 1},
		{"0/1000", "0/1000", 0},
		{"0/1000", "0/2000", -1},
		{"0/2000", "0/1000", 1},
		{"1/0", "0/FFFFFFFF", 1},
	}
	for _, tc := range cases {
		got, err := CompareLSN(tc.a, tc.b)
		if err != nil {
			t.Fatalf("CompareLSN(%q,%q): %v", tc.a, tc.b, err)
		}
		if got != tc.want {
			t.Errorf("CompareLSN(%q,%q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestMaxLSN(t *testing.T) {
	got, err := maxLSN("0/1000", "0/2000")
	if err != nil || got != "0/2000" {
		t.Errorf("maxLSN = %q, %v", got, err)
	}
	got, err = maxLSN("0/2000", "0/1000")
	if err != nil || got != "0/2000" {
		t.Errorf("maxLSN = %q, %v", got, err)
	}
	got, err = maxLSN("", "0/1")
	if err != nil || got != "0/1" {
		t.Errorf("maxLSN(empty) = %q, %v", got, err)
	}
}
