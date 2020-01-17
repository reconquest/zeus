package zfs

import (
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/constants"
	"github.com/reconquest/zeus/pkg/exec"
	"github.com/reconquest/zeus/pkg/log"
)

type CopyProgress struct {
	StartedAt time.Time

	TotalSize uint64
	SentSize  uint64
	Sent      bool
}

func CopyDataset(
	sourceSnapshot string,
	targetDataset string,
	baseSnapshot string,
	progressFunc func(CopyProgress),
) error {
	sendArgs := []string{
		`send`, `-P`, `-c`, sourceSnapshot,
	}

	if baseSnapshot != "" {
		sendArgs = append(sendArgs, `-i`, baseSnapshot)
	}

	var (
		recv = exec.Exec(
			`zfs`, `recv`, `-u`,
			fmt.Sprintf(
				"%s/%s",
				targetDataset,
				sourceSnapshot,
			),
		)

		send = exec.Exec(`zfs`, sendArgs...).NoStdLog()
	)

	stdout, err := send.StdoutPipe()
	if err != nil {
		return err
	}

	progress := CopyProgress{
		StartedAt: time.Now(),
	}

	sizeUsed, sizeReferenced, err := getSize(sourceSnapshot)
	if err != nil {
		return err
	}

	if baseSnapshot == "" {
		progress.TotalSize = sizeReferenced
	} else {
		progress.TotalSize = sizeUsed
	}

	var (
		progressWriter = ProgressWriter{}
		progressDone   = make(chan struct{}, 1)
	)

	go func() {
		for {
			progress.SentSize = progressWriter.Total
			progressFunc(progress)

			select {
			case <-progressDone:
				return
			default:
				time.Sleep(time.Second)
			}
		}
	}()

	defer func() {
		progressDone <- struct{}{}
	}()

	recv.SetStdin(io.TeeReader(stdout, &progressWriter))

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

		log.Info("{copy} send thread finished, now waiting for receive")
	}()

	err = recv.Run()
	if err != nil {
		errs.Lock()
		errs.reasons = append(errs.reasons, karma.Format(
			err,
			"zfs receive failed",
		))
		errs.Unlock()

		send.Process().Kill()
	}

	wg.Wait()

	if len(errs.reasons) > 0 {
		return karma.Push(err, errs.reasons...)
	}

	return nil
}

func getSize(snapshot string) (uint64, uint64, error) {
	mappings, err := GetDatasetProperties([]PropertyRequest{
		{Name: constants.Used, System: true, Snapshot: true},
		{Name: constants.Referenced, System: true, Snapshot: true},
	}, snapshot)
	if err != nil {
		return 0, 0, karma.Format(
			err,
			"unable to get used & referenced sizes for %q",
			snapshot,
		)
	}

	var (
		sizeUsed       uint64
		sizeReferenced uint64
	)

	if len(mappings) == 0 {
		return 0, 0, karma.
			Describe("snapshot", snapshot).
			Reason(
				"given dataset %q is not found",
			)
	}

	for _, mapping := range mappings {
		if mapping.Source != snapshot {
			continue
		}

		for _, property := range mapping.Properties {
			switch property.Name {
			case constants.Used:
				size, err := strconv.ParseUint(property.Value, 10, 64)
				if err != nil {
					return 0, 0, karma.
						Describe("size", property.Value).
						Format(
							err,
							"unable to parse used size",
						)
				}

				sizeUsed = size

			case constants.Referenced:
				size, err := strconv.ParseUint(property.Value, 10, 64)
				if err != nil {
					return 0, 0, karma.
						Describe("size", property.Value).
						Format(
							err,
							"unable to parse referenced size",
						)
				}

				sizeReferenced = size
			}
		}
	}

	return sizeUsed, sizeReferenced, nil
}
