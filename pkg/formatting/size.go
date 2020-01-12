package formatting

import "fmt"

func Size(size uint64) string {
	const unit = 1000
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}

	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB",
		float64(size)/float64(div), "kMGTPE"[exp])
}
