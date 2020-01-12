package backup

import (
	"io"
	"sync"
	"time"

	"github.com/reconquest/callbackwriter-go"
	"github.com/reconquest/lineflushwriter-go"
	"github.com/reconquest/nopio-go"
)

type RecvStatusWriter struct {
	backend io.Writer

	size struct {
		full      int64
		estimated int64
	}

	progress struct {
		started time.Time
		updated time.Time
		sent    int64
	}
}

func NewRecvStatusWriter() *RecvStatusWriter {
	var writer RecvStatusWriter

	writer.backend = lineflushwriter.New(
		callbackwriter.New(
			nopio.NopWriteCloser{},
			func(line []byte) {
			},
			nil,
		),
		&sync.Mutex{},
		true,
	)

	return &writer
}
