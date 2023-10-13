package merge_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/lietoast/external-sort/merge"
	"github.com/lietoast/external-sort/preprocessing"
)

type element int

func (e element) Less(x preprocessing.Lesser) bool {
	return e < x.(element)
}

func (e element) String() string {
	return fmt.Sprintf("%d", int(e))
}

func TestRaceTree(t *testing.T) {
	elements := make([]preprocessing.FileRecord, 0)

	for i := 0; i < 10; i++ {
		rnum := rand.Intn(100)
		elements = append(elements, element(rnum))
	}

	loserTree := new(merge.LoserTree)
	loserTree.InitLoserTree(elements)

	for i := 0; i < 10; i++ {
		fmt.Println(elements)
		fmt.Println(loserTree.GetWinner())

		rnum := rand.Intn(100)
		elements[loserTree.GetWinner()] = element(rnum)
		loserTree.Adjust(elements, loserTree.GetWinner())
	}
}
