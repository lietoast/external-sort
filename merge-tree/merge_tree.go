package merge_tree

import (
	"container/heap"
	"fmt"
	"os"

	"github.com/lietoast/external-sort/preprocessing"
)

// 最佳合并树的结点
type MergeTreeNode struct {
	RunLength *os.File // 游程文件
	LineNum   uint64   // 游程文件的行数
}

// 用于构建最佳合并树的优先队列
type MergeTreePrioQueue []MergeTreeNode

func (mtpq MergeTreePrioQueue) Len() int {
	return len(mtpq)
}

func (mtpq MergeTreePrioQueue) Swap(i, j int) {
	mtpq[i], mtpq[j] = mtpq[j], mtpq[i]
}

func (mtpq MergeTreePrioQueue) Less(i, j int) bool {
	return mtpq[i].LineNum < mtpq[j].LineNum
}

func (mtpq *MergeTreePrioQueue) Push(node interface{}) {
	*mtpq = append(*mtpq, node.(MergeTreeNode))
}

func (mtpq *MergeTreePrioQueue) Pop() interface{} {
	idx := len(*mtpq) - 1
	res := (*mtpq)[idx]
	*mtpq = (*mtpq)[:idx]
	return res
}

// 构造用于生成最佳归并树(哈夫曼树)的优先队列(森林)
func BuildMergeTreePrioQueue(nodes map[string]uint64, K int) (MergeTreePrioQueue, error) {
	// 构造森林
	huffman := make(MergeTreePrioQueue, 0)

	for fname, nl := range nodes {
		fp, err := os.OpenFile(fname, os.O_RDONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("run length %s lost: %s", fname, err.Error())
		}

		hnode := MergeTreeNode{
			RunLength: fp,
			LineNum:   nl,
		}
		heap.Push(&huffman, hnode)
	}

	// 补齐虚游程
	if K == 1 || (len(nodes)-1)%(K-1) == 0 {
		return huffman, nil
	}

	for i := 0; i < K-1-(len(nodes)-1)%(K-1); i++ {
		fp, err := os.CreateTemp(preprocessing.RunLengthDir, "esort_*_abstract.rl")
		if err != nil {
			return nil, err
		}

		hnode := MergeTreeNode{
			RunLength: fp,
			LineNum:   0,
		}
		heap.Push(&huffman, hnode)
	}

	return huffman, nil
}
