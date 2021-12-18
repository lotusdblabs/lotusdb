package lotusdb

import (
	cmap "github.com/orcaman/concurrent-map"
	"testing"
)

func TestOpen(t *testing.T) {
	cmap := cmap.New()
	cmap.Set("a", 1)

}
