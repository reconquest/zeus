package zfs

type ProgressWriter struct {
	Total uint64
}

func (writer *ProgressWriter) Write(data []byte) (int, error) {
	writer.Total += uint64(len(data))

	return len(data), nil
}
