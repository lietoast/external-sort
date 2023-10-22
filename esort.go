package external_sort

import (
	"fmt"

	"github.com/lietoast/external-sort/merge"
	mgtree "github.com/lietoast/external-sort/merge-tree"
	"github.com/lietoast/external-sort/preprocessing"
)

// 默认情况下, 内存大小为512MB, filename为需要对其排序的磁盘文件, recordSize为文件中一条记录的大小
// 每一条记录在磁盘文件中占据一行, resultFilePath为排好序的文件的路径
// converter为将一条文本数据转换为FileRecord类型数据的工具
// 返回: 排序过程中发生的错误
func Default(filename string, recordSize uint64, resultFilePath string, converter preprocessing.Converter) error {
	// 执行预处理
	runLengthNames, err := preprocessing.PreprocessingProcedure(
		filename,
		uint64(1)<<29,
		recordSize,
		preprocessing.NewLocalFileReader(),
		converter,
		preprocessing.READ_LINE,
	)
	if err != nil || len(runLengthNames) <= 0 {
		return err
	}
	fmt.Println("preprocessing finished")

	N := len(runLengthNames)
	var K int

	if N <= 3 { // 决定合并路数
		K = N
	} else if N <= 20 {
		K = 3
	} else {
		K = 5
	}

	// 获取最佳合并树
	mtpq, err := mgtree.BuildMergeTreePrioQueue(runLengthNames, K)
	if err != nil {
		return err
	}
	fmt.Println("merge tree has been built:", mtpq)

	// 合并游程
	err = merge.Merge(mtpq, resultFilePath, K, preprocessing.NewLocalFileReader(), converter, preprocessing.READ_LINE, recordSize)
	fmt.Println("finished merging")

	return err
}
