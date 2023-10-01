package preprocessing_test

import (
	"sync"
	"testing"

	"github.com/lietoast/external-sort/preprocessing"
	"github.com/lietoast/external-sort/test/ds"
)

func TestPreprocessingProcedure(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := preprocessing.PreprocessingProcedure(
			"./kvs",
			1024,
			16,
			preprocessing.NewLocalFileReader(),
			ds.KVConverter{},
			preprocessing.READ_LINE,
		)
		if err != nil {
			t.Errorf("procedure failure: %s", err.Error())
		}
	}()
	wg.Wait()
}
