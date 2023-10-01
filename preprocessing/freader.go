package preprocessing

import (
	"bufio"
	"io"
	"os"
	"sync"
)

// 从文件中读取指定类型的数据
type FReader interface {
	// 尝试从stream中读取nmem个msize大小的数据
	// 将读取到的数据转化后返回
	// 返回值:
	// 转化好的数据, 成功读取到的数据个数
	Fread(stream *os.File, msize uint64, nmem uint64, cvt Converter) ([]FileRecord, uint64)
	// 尝试从stream中读取lineNum行, 将每一行当作一条数据进行转化
	// 返回成功读取到的行数
	ReadLines(stream *os.File, lineNum uint64, cvt Converter) ([]FileRecord, uint64)
}

// 普通本地文件读取器
type LocalFileReader struct {
	stream    *os.File
	reader    *bufio.Reader
	readerMtx *sync.Mutex
}

func (lfr *LocalFileReader) Fread(stream *os.File, msize uint64, nmem uint64, cvt Converter) ([]FileRecord, uint64) {
	buf := make([]byte, msize)
	res := make([]FileRecord, 0)

	for i := uint64(0); i < nmem; i++ {
		_, err := io.ReadFull(stream, buf)
		if err != nil {
			return res, i
		}

		data, err := cvt.Convert(string(buf))
		if err != nil {
			return res, i
		}
		res = append(res, data)
	}

	return res, nmem
}

func (lfr *LocalFileReader) ReadLines(stream *os.File, lineNum uint64, cvt Converter) ([]FileRecord, uint64) {
	reader := lfr.GetReader(stream)
	res := make([]FileRecord, 0)

	for i := uint64(0); i < lineNum; i++ {
		s, err := reader.ReadSlice('\n')
		if err != nil {
			return res, i
		}

		data, err := cvt.Convert(string(s[:len(s)-1]))
		if err != nil {
			return res, i
		}
		res = append(res, data)
	}

	return res, lineNum
}

func (lfr *LocalFileReader) GetReader(stream *os.File) *bufio.Reader {
	if lfr.stream == nil || lfr.stream != stream {
		lfr.readerMtx.Lock()
		if lfr.stream == nil || lfr.stream != stream {
			lfr.stream = stream
			lfr.reader = bufio.NewReader(stream)
		}
		lfr.readerMtx.Unlock()
	}
	return lfr.reader
}

func NewLocalFileReader() *LocalFileReader {
	reader := new(LocalFileReader)
	reader.stream = nil
	reader.reader = nil
	reader.readerMtx = new(sync.Mutex)
	return reader
}
