package ds

import (
	"fmt"

	"github.com/lietoast/external-sort/preprocessing"
)

type KV struct {
	word string
	freq int
}

func (kv KV) Less(lesser preprocessing.Lesser) bool {
	return kv.freq < lesser.(KV).freq
}

func (kv KV) String() string {
	return fmt.Sprintf("%s %d", kv.word, kv.freq)
}

type KVConverter struct{}

func (c KVConverter) Convert(memb string) (preprocessing.FileRecord, error) {
	var kv KV

	n, err := fmt.Sscanf(memb, "%s %d", &kv.word, &kv.freq)
	if n < 2 || err != nil {
		return nil, err
	}

	return kv, nil
}
