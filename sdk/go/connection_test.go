package nanocached

import (
	"errors"
	"math"
	"strconv"
	"testing"
)

// TestParseStrictUintRejectsAnythingButDigits is issue #462: every
// integer field the wire protocol sends (lengths, counts, tags, TTLs)
// must accept ASCII digits only — ^[0-9]+$ — rejecting a leading `+`,
// leading/trailing whitespace, `_` digit-group separators, an exponent,
// a leading `-`, and empty input, while still accepting leading zeros
// ("007") since the server's own parse_length grammar (src/command.rs)
// allows them.
func TestParseStrictUintRejectsAnythingButDigits(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    uint64
		wantErr bool
	}{
		{"plain digit", "5", 5, false},
		{"zero", "0", 0, false},
		{"leading zeros", "007", 7, false},
		{"multi-digit", "12345", 12345, false},
		{"leading plus", "+5", 0, true},
		{"leading space", " 5", 0, true},
		{"trailing space", "5 ", 0, true},
		{"underscore digit group", "1_000", 0, true},
		{"exponent", "1e2", 0, true},
		{"leading minus", "-5", 0, true},
		{"empty", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStrictUint(tt.in, 64)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseStrictUint(%q) = %d, nil, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStrictUint(%q) = %v, want %d, nil", tt.in, err, tt.want)
			}
			if got != tt.want {
				t.Fatalf("parseStrictUint(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestParseStrictIntSameGrammarAsUintAndBoundsToInt checks parseStrictInt
// shares parseStrictUint's grammar (issue #462) and additionally rejects
// a value that would overflow int, mirroring strconv.Atoi's own overflow
// contract.
func TestParseStrictIntSameGrammarAsUintAndBoundsToInt(t *testing.T) {
	for _, tt := range []struct {
		name    string
		in      string
		want    int
		wantErr bool
	}{
		{"plain digit", "5", 5, false},
		{"leading zeros", "007", 7, false},
		{"leading plus", "+5", 0, true},
		{"leading space", " 5", 0, true},
		{"trailing space", "5 ", 0, true},
		{"underscore digit group", "1_000", 0, true},
		{"exponent", "1e2", 0, true},
		{"leading minus", "-5", 0, true},
		{"empty", "", 0, true},
		{"overflows int64", "99999999999999999999", 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStrictInt(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseStrictInt(%q) = %d, nil, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStrictInt(%q) = %v, want %d, nil", tt.in, err, tt.want)
			}
			if got != tt.want {
				t.Fatalf("parseStrictInt(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestParseStrictInt64BoundaryAtMaxInt64 confirms the existing
// range/overflow behavior at the int64 boundary is unchanged by the
// digits-only grammar check: math.MaxInt64 itself parses, one past it
// doesn't (it's a legal digit string, just out of range for int64,
// exactly like strconv.ParseInt's own contract).
func TestParseStrictInt64BoundaryAtMaxInt64(t *testing.T) {
	maxStr := strconv.FormatInt(math.MaxInt64, 10)
	got, err := parseStrictInt64(maxStr)
	if err != nil || got != math.MaxInt64 {
		t.Fatalf("parseStrictInt64(%q) = %d, %v, want %d, nil", maxStr, got, err, int64(math.MaxInt64))
	}

	overStr := "9223372036854775808" // math.MaxInt64 + 1
	if _, err := parseStrictInt64(overStr); err == nil {
		t.Fatalf("parseStrictInt64(%q) = nil error, want a range error", overStr)
	}

	// The digits-only grammar still applies at the boundary: a `+`-prefixed
	// in-range value must still be rejected, not silently accepted because
	// it happens to fit.
	if _, err := parseStrictInt64("+" + maxStr); err == nil {
		t.Fatalf("parseStrictInt64(%q) = nil error, want rejection of the leading +", "+"+maxStr)
	}
}

// TestParseCounterValueAllowsOnlyALeadingMinus is the one documented
// exception (issue #462): an `I` response's counter body allows exactly
// one optional leading `-` (never `+`), the same grammar the request's
// own <delta> field uses (appendIncrFrame) — mirroring Python's
// `_INCR_VALUE_RE = re.compile(rb"-?[0-9]{1,19}")` and .NET's
// TryParseWireCounter/ParseTag split.
func TestParseCounterValueAllowsOnlyALeadingMinus(t *testing.T) {
	for _, tt := range []struct {
		name    string
		in      string
		want    int64
		wantErr bool
	}{
		{"plain digit", "5", 5, false},
		{"leading zeros", "007", 7, false},
		{"leading minus", "-5", -5, false},
		{"negative with leading zeros", "-007", -7, false},
		{"leading plus", "+5", 0, true},
		{"leading space", " 5", 0, true},
		{"trailing space", "5 ", 0, true},
		{"underscore digit group", "1_000", 0, true},
		{"exponent", "1e2", 0, true},
		{"empty", "", 0, true},
		{"double minus", "--5", 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCounterValue(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseCounterValue(%q) = %d, nil, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseCounterValue(%q) = %v, want %d, nil", tt.in, err, tt.want)
			}
			if got != tt.want {
				t.Fatalf("parseCounterValue(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestParseTagRejectsLeadingPlus is issue #462's site-specific check for
// parseTag (connection.go), on top of the shared-helper table above:
// echoed response tags must reject a `+`-prefixed value and surface
// ErrProtocol, exactly like any other malformed header field.
func TestParseTagRejectsLeadingPlus(t *testing.T) {
	if _, err := parseTag("+5"); !errors.Is(err, ErrProtocol) {
		t.Fatalf("parseTag(%q) = %v, want ErrProtocol", "+5", err)
	}
	tag, err := parseTag("5")
	if err != nil || tag != 5 {
		t.Fatalf("parseTag(%q) = %d, %v, want 5, nil", "5", tag, err)
	}
}
