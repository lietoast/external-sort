package preprocessing

import (
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	READ_LINE = 0
	READ_SIZE = 1

	routineNum = 3
)

// 外排序第一阶段(预处理)的过程
// 将大文件中包含的n个无序记录预处理成多个有序子文件, 这些子文件成为初始游程
// 采取置换选择算法完成这一步骤

// 生成游程文件
func PreprocessingProcedure(file string, memorySize,
	recordSize uint64, reader FReader,
	cvt Converter, readMethod int) error {

	fp, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer fp.Close()

	// 内存中最多可以容纳的记录数量
	N := memorySize / recordSize
	// 一次性读取内存最多可以容纳的数量的记录
	records, err := readRecords(N, fp, recordSize, reader, readMethod, cvt)
	if err != nil {
		return err
	}
	// 建立初始堆
	sorter, err := NewReplacementSelectionSorter(N, records)
	if err != nil {
		return err
	}

	// 第二阶段: 输入/输出
	var wg sync.WaitGroup
	wg.Add(routineNum)

	newRecords := make(chan FileRecord, 100)
	output := make(chan string, 100)
	flushSig := make(chan struct{}, 1)

	go func() {
		count := 0
		defer wg.Done()
		for {
			record, err := readRecords(1, fp, recordSize, reader, readMethod, cvt)
			if err != nil {
				break
			}
			count++

			newRecords <- record[0]
		}
		close(newRecords)
	}()

	go func() {
		defer wg.Done()
		for record := range newRecords {
			sorter.Output(output, record)
		}
		flushSig <- struct{}{}
	}()

	go func() {
		defer wg.Done()
		for msg := range output {
			sorter.curHeapMtx.Lock()
			fmt.Fprintln(sorter.currentRunLength, msg)
			sorter.curHeapMtx.Unlock()
		}
	}()

	<-flushSig
	sorter.Flush(output)
	close(output)

	wg.Wait()

	sorter.currentRunLength.Close()

	return nil
}

func readRecords(N uint64, fp *os.File, recordSize uint64, reader FReader, readMethod int, cvt Converter) ([]FileRecord, error) {
	// 记录
	records := make([]FileRecord, 0)
	// 已经读取的记录数量
	cnt := uint64(0)

	switch readMethod {
	case READ_LINE:
		for cnt < N {
			rcs, c := reader.ReadLines(fp, N, cvt)
			if c <= 0 {
				return records, io.EOF
			}

			records = append(records, rcs...)
			cnt += c
		}
	case READ_SIZE:
		for cnt < N {
			rcs, c := reader.Fread(fp, recordSize, N, cvt)
			if c <= 0 {
				return records, io.EOF
			}

			records = append(records, rcs...)
			cnt += c
		}
	default:
		return nil, fmt.Errorf("illegal reading method")
	}

	return records, nil
}
