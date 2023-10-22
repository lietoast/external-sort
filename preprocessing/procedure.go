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
	cvt Converter, readMethod int) (map[string]uint64, error) {

	fp, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	// 内存中最多可以容纳的记录数量
	N := memorySize / recordSize
	// 一次性读取内存最多可以容纳的数量的记录
	records, err := readRecords(N, fp, recordSize, reader, readMethod, cvt)
	if err != nil {
		return nil, err
	}
	// 建立初始堆
	sorter, output := NewReplacementSelectionSorter(N, records)

	// 第二阶段: 输入/输出
	var wg sync.WaitGroup
	wg.Add(routineNum)

	newRecords := make(chan FileRecord, 100)
	flushSig := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			record, err := readRecords(1, fp, recordSize, reader, readMethod, cvt)
			if err != nil {
				break
			}

			newRecords <- record[0]
		}
		close(newRecords)
	}()

	go func() {
		defer wg.Done()
		for record := range newRecords {
			sorter.Output(record)
		}
		flushSig <- struct{}{}
	}()

	tmpfiles := make(map[string]uint64)

	go func() {
		defer wg.Done()

		tmpfile, _ := os.CreateTemp(RunLengthDir, "esort_*.rl")
		tmpfiles[tmpfile.Name()] = 0

		for msg := range output {
			if msg == "\n" {
				tmpfile.Close()
				tmpfile, _ = os.CreateTemp(RunLengthDir, "esort_*.rl")
				tmpfiles[tmpfile.Name()] = 0
				continue
			}

			fmt.Fprint(tmpfile, msg)
			tmpfiles[tmpfile.Name()] += 1
		}

		tmpfile.Close()
	}()

	<-flushSig
	sorter.Flush()

	wg.Wait()

	return tmpfiles, nil
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

// 读取文件一次
func ReadOnce(file *os.File, reader FReader, cvt Converter, rs uint64, method int) (FileRecord, error) {
	records, err := readRecords(1, file, rs, reader, method, cvt)
	if err != nil {
		return nil, err
	}
	return records[0], err
}
