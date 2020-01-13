package zfs

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/reconquest/callbackwriter-go"
	"github.com/reconquest/karma-go"
	"github.com/reconquest/lineflushwriter-go"
	"github.com/reconquest/nopio-go"
	"github.com/reconquest/zeus/pkg/exec"
)

type CopyProgress struct {
	SendType string

	OriginalSize  uint64
	EstimatedSize uint64

	Started  time.Time
	Updated  time.Time
	SentSize uint64

	Sent bool

	Error error
}

func CopyDataset(
	sourceSnapshot string,
	targetDataset string,
	baseSnapshot string,
	progressFunc func(line CopyProgress),
) error {
	sendArgs := []string{
		`send`, `-Pvc`, sourceSnapshot,
	}

	if baseSnapshot != "" {
		sendArgs = append(sendArgs, `-i`, baseSnapshot)
	}

	var (
		recv = exec.Exec(
			`zfs`, `recv`, `-F`, fmt.Sprintf(
				"%s/%s",
				targetDataset,
				sourceSnapshot,
			),
		)

		send = exec.Exec(`zfs`, sendArgs...).NoStdLog()
	)

	progress := CopyProgress{
		Started: time.Now(),
	}

	logger := lineflushwriter.New(
		callbackwriter.New(
			nopio.NopWriteCloser{},
			func(line []byte) {
				progressFunc(progress.apply(string(line)))
			},
			nil,
		),
		&sync.Mutex{},
		true,
	)

	stdout, err := send.StdoutPipe()
	if err != nil {
		return err
	}

	send.SetStderr(logger)
	recv.SetStdin(stdout)

	var wg sync.WaitGroup
	var errs struct {
		sync.Mutex
		reasons []karma.Reason
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = send.Run()
		if err != nil {
			errs.Lock()
			errs.reasons = append(errs.reasons, karma.Format(
				err,
				"zfs send failed",
			))
			errs.Unlock()
		}

		progress.Sent = true
		progressFunc(progress)
	}()

	err = recv.Run()
	if err != nil {
		errs.Lock()
		errs.reasons = append(errs.reasons, karma.Format(
			err,
			"zfs receive failed (start)",
		))
		errs.Unlock()

		send.Process().Kill()
	}

	wg.Wait()

	return karma.Push(err, errs.reasons...)
}

func (progress *CopyProgress) apply(line string) CopyProgress {
	progress.Error = progress.try(line)

	return *progress
}

func (progress *CopyProgress) try(line string) error {
	fields := strings.Split(strings.Trim(line, "\n"), "\t")

	switch fields[0] {
	case "incremental":
		fallthrough
	case "full":
		progress.SendType = fields[0]

		if len(fields) != 3 {
			return karma.
				Describe("line", line).
				Reason(
					"unexpected number of fields in first line of `zfs send` output",
				)
		}

		size, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return karma.
				Describe("line", line).
				Format(
					err,
					"unable to parse size value from first line of `zfs send` output",
				)
		}

		progress.OriginalSize = size

	case "size":
		if len(fields) != 2 {
			return karma.
				Describe("line", line).
				Reason(
					"unexpected number of fields in second line of `zfs send` output",
				)
		}

		size, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return karma.
				Describe("line", line).
				Format(
					err,
					"unable to parse size value from second line of `zfs send` output",
				)
		}

		progress.EstimatedSize = size

	default:
		size, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return karma.
				Describe("line", line).
				Format(
					err,
					"unable to parse size value from second line of `zfs send` output",
				)
		}

		progress.SentSize = size
	}

	progress.Updated = time.Now()

	return nil
}
