package preprocessing_test

import (
	"testing"

	"github.com/lietoast/external-sort/preprocessing"
	"github.com/lietoast/external-sort/test/ds"
)

func TestPreprocessingProcedure(t *testing.T) {
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
}
