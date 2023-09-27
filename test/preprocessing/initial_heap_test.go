package preprocessing_test

import (
	"container/heap"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lietoast/external-sort/preprocessing"
)

type element int

func (e element) Less(x preprocessing.Lesser) bool {
	return e < x.(element)
}

func (e element) String() string {
	return fmt.Sprintf("%d", int(e))
}

func TestHeap(t *testing.T) {
	h := make(preprocessing.FRecordHeap, 0)

	for i := 0; i < 10; i++ {
		rnum := rand.Intn(100)
		fmt.Printf("%d ", rnum)

		h.Push(element(rnum))
	}
	fmt.Println()

	heap.Init(&h)

	for i := 0; i < 10; i++ {
		fmt.Printf("%d ", heap.Pop(&h).(element))
	}
	fmt.Println()
}
