package preprocessing

// 将字符串转化为文件记录类型的数据
type Converter interface {
	Convert(memb string) (FileRecord, error)
}
