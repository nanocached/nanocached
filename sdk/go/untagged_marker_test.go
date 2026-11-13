package nanocached

import (
	"errors"
	"testing"
)

// Issue #1859: the byte after an untagged no-body marker was read and not
// checked, so `Sx` passed as `S` and left the stream misaligned.
func TestAnUntaggedNoBodyMarkerMustEndWithANewline(t *testing.T) {
	for _, marker := range "BSDNWCRT" {
		ok := newReaderOnlyConnection(string(marker)+"\n", false)
		got, _, _, _, _, err := ok.readOneResponse()
		if err != nil || got != byte(marker) {
			t.Errorf("%c\\n: marker %q, err %v", marker, got, err)
		}

		bad := newReaderOnlyConnection(string(marker)+"x\n", false)
		if _, _, _, _, _, err := bad.readOneResponse(); !errors.Is(err, ErrProtocol) {
			t.Errorf("%cx: err %v, want a protocol error", marker, err)
		}
	}
}
