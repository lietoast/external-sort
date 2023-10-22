package merge

import "github.com/lietoast/external-sort/preprocessing"

// 构建一个简单易用的败者树, 包括创建/输出(也就是重构)两种操作

type LoserTree struct {
	nodes []int
}

func (lt *LoserTree) InitLoserTree(records []preprocessing.FileRecord) {
	n := len(records)

	lt.nodes = make([]int, n)

	for i := 0; i < n; i++ {
		lt.nodes[i] = -1
	}

	for i := n - 1; i >= 0; i-- {
		lt.Adjust(records, i)
	}
}

func (lt *LoserTree) Adjust(records []preprocessing.FileRecord, s int) {
	end := len(records)
	t := (s + end) / 2
	var tmp int

	for t != 0 {
		if s == -1 {
			break
		}

		if lt.nodes[t] == -1 || !less(records[s], records[lt.nodes[t]]) {
			tmp = s
			s = lt.nodes[t]
			lt.nodes[t] = tmp
		}

		t /= 2
	}

	lt.nodes[0] = s
}

func (lt LoserTree) GetWinner() int {
	return lt.nodes[0]
}

func less(x, y preprocessing.FileRecord) bool {
	if x == nil && y == nil {
		return true
	} else if x == nil {
		return false
	} else if y == nil {
		return true
	}

	return x.Less(y)
}
