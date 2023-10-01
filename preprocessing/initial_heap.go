package preprocessing

import (
	"container/heap"
	"fmt"
	"sync"
)

const RunLengthDir = "/Users/lietoast/RunLength" // 存放游程的目录

// 可比较类型
type Lesser interface {
	// 比较当前Lesser类型变量与传入的Lesser类型变量的大小
	// 返回当前类型变量是否比传入的变量小
	Less(lesser Lesser) bool
}

type FileRecord interface {
	Lesser
	fmt.Stringer
}

// 文件记录的最小堆
type FRecordHeap []FileRecord

func (frh FRecordHeap) Len() int {
	return len(frh)
}

func (frh FRecordHeap) Swap(i, j int) {
	frh[i], frh[j] = frh[j], frh[i]
}

func (frh FRecordHeap) Less(i, j int) bool {
	return frh[i].Less(frh[j])
}

func (frh *FRecordHeap) Push(record interface{}) {
	*frh = append(*frh, record.(FileRecord))
}

func (frh *FRecordHeap) Pop() interface{} {
	r := (*frh)[len(*frh)-1]
	*frh = (*frh)[:len(*frh)-1]
	return r
}

type ReplacementSelectionSorter struct {
	maximumRecordNum uint64       // 内存所能容纳的文件记录的最大数量
	currentHeap      *FRecordHeap // 当前堆
	output           chan string  // 输出
	newHeap          *FRecordHeap // 新堆
	adjustHeap       *sync.Once   // 调整新堆的操作只进行一次
	curHeapMtx       sync.Mutex   // 锁住当前堆(其实主要是锁住当前游程)
	newHeapMtx       sync.Mutex   // 锁住新堆
}

// 建立初始堆
func (rss *ReplacementSelectionSorter) BuildInitialHeap(N uint64, records []FileRecord) {
	rss.maximumRecordNum = N

	h := FRecordHeap(records)
	heap.Init(&h) // 建立初始堆
	rss.currentHeap = &h

	rss.output = make(chan string, 100)
}

func NewReplacementSelectionSorter(N uint64, records []FileRecord) (*ReplacementSelectionSorter, chan string) {
	rss := new(ReplacementSelectionSorter)

	rss.BuildInitialHeap(N, records)

	rss.newHeap = nil
	rss.curHeapMtx = sync.Mutex{}
	rss.newHeapMtx = sync.Mutex{}

	return rss, rss.output
}

// 输出堆顶元素, 并且补充新的元素
func (rss *ReplacementSelectionSorter) Output(newRecord FileRecord) {
	rss.output <- (*rss.currentHeap)[0].String() // 将栈顶元素输出

	// 比较刚才输出的元素与新元素
	if (*rss.currentHeap)[0].Less(newRecord) { // 如果新元素大于等于栈顶元素
		// 那么新元素成为新的栈顶元素
		(*rss.currentHeap)[0] = newRecord
		// 并且调整当前堆
		heap.Fix(rss.currentHeap, 0)
	} else { // 反之, 将新元素加入新堆中
		heap.Pop(rss.currentHeap)

		rss.newHeapMtx.Lock()

		if rss.newHeap == nil {
			h := FRecordHeap([]FileRecord{newRecord})
			rss.newHeap = &h
			rss.adjustHeap = new(sync.Once)
		} else {
			rss.newHeap.Push(newRecord)
		}

		// 如果新堆内的元素数量占一半内存大小以上, 则对其进行调整
		if uint64(rss.newHeap.Len()) >= (rss.maximumRecordNum >> 1) {
			rss.adjustHeap.Do(func() {
				heap.Init(rss.newHeap)
			})
			heap.Fix(rss.newHeap, rss.newHeap.Len()-1)
		}

		// 如果新堆占据了整个内存, 则将其转变为当前堆
		if uint64(rss.newHeap.Len()) >= rss.maximumRecordNum {
			rss.curHeapMtx.Lock()

			rss.currentHeap = rss.newHeap
			rss.newHeap = nil

			rss.output <- "\n" // 换行符代表更换游程文件

			rss.curHeapMtx.Unlock()
		}

		rss.newHeapMtx.Unlock()
	}
}

// 清空当前堆和新堆
func (rss *ReplacementSelectionSorter) Flush() {
	// 清空当前堆
	for rss.currentHeap.Len() > 0 {
		rss.output <- (*rss.currentHeap)[0].String()
		heap.Pop(rss.currentHeap)
	}

	// 清空新堆
	if rss.newHeap == nil || rss.newHeap.Len() <= 0 {
		return
	}

	rss.output <- "\n"

	for rss.newHeap.Len() > 0 {
		rss.output <- (*rss.newHeap)[0].String()
		heap.Pop(rss.newHeap)
	}

	// 关闭输出
	close(rss.output)
}
