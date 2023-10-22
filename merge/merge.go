package merge

import (
	"container/heap"
	"fmt"
	"os"
	"sync"

	. "github.com/lietoast/external-sort/merge-tree"
	pp "github.com/lietoast/external-sort/preprocessing"
)

// 将K个游程文件合并到指定的文件中
func MergeRunLengths(runLengths []MergeTreeNode, result *os.File,
	reader pp.FReader, cvt pp.Converter, readingMethod int, recordSize uint64) {
	// 创建用于传递数据的管道
	k := len(runLengths)

	// 游程文件 -> 竞赛树
	datas := []chan pp.FileRecord{}
	for i := 0; i < k; i++ {
		datas = append(datas, make(chan pp.FileRecord, 10))
	}

	var wg sync.WaitGroup
	wg.Add(k + 1)

	// 开启K个协程, 从游程文件中读取数据
	for i := 0; i < k; i++ {
		go func(rl MergeTreeNode, output chan pp.FileRecord) {
			defer wg.Done()
			for {
				record, err := pp.ReadOnce(rl.RunLength, reader, cvt, recordSize, readingMethod)
				if err != nil {
					output <- nil
					close(output)
					break
				}
				output <- record
			}
		}(runLengths[i], datas[i])
	}

	records := make([]pp.FileRecord, k)
	loserTree := new(LoserTree)

	for i := 0; i < k; i++ {
		records[i] = <-datas[i]
	}
	// ####################################
	fmt.Println(records)
	loserTree.InitLoserTree(records)

	// 开启一个协程, 将数据排序并写入到文件中
	go func() {
		defer wg.Done()

		for {
			index := loserTree.GetWinner()
			if records[index] == nil {
				return
			}

			fmt.Fprint(result, records[index].String())

			records[index] = <-datas[index]
			loserTree.Adjust(records, index)
		}
	}()

	wg.Wait()
}

// 合并所有游程文件
func Merge(mergeTree MergeTreePrioQueue, sortedFileName string, k int,
	reader pp.FReader, cvt pp.Converter, readingMethod int, recordSize uint64) error {
	// 开启合并后排序好的文件
	result, err := os.OpenFile(sortedFileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer result.Close()

	var runLengths []MergeTreeNode

	for {
		// 当前待合并的k个游程
		runLengths = make([]MergeTreeNode, 0)

		// 从最佳合并树中取出当前要合并的k个游程
		for i := 0; i < k; i++ {
			runLengths = append(runLengths, heap.Pop(&mergeTree).(MergeTreeNode))
		}
		if mergeTree.Len() <= 0 {
			break
		}

		// 添加新的游程文件
		newRunLength, err := os.CreateTemp(pp.RunLengthDir, "esort_*.rl")
		if err != nil {
			return err
		}

		// 合并文件
		MergeRunLengths(
			runLengths,
			newRunLength,
			reader,
			cvt,
			readingMethod,
			recordSize,
		)

		newNode := MergeTreeNode{
			RunLength: newRunLength,
			LineNum:   0,
		}

		// 关闭游程文件
		for i := 0; i < k; i++ {
			runLengths[i].RunLength.Close()
			newNode.LineNum += runLengths[i].LineNum
		}

		// 将新的游程文件添加到合并树中
		heap.Push(&mergeTree, newNode)
	}

	// 最后一次合并直接将游程合并到结果文件中
	MergeRunLengths(
		runLengths,
		result,
		reader,
		cvt,
		readingMethod,
		recordSize,
	)
	for i := 0; i < k; i++ {
		runLengths[i].RunLength.Close()
	}

	result.Close()

	return nil
}
