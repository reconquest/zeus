package zfs

import (
	"fmt"
	"io"
	"strconv"
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

	sizeWritten, sizeReferenced, err := getSize(sourceSnapshot)
	if err != nil {
		return err
	}

	if baseSnapshot == "" {
		progress.TotalSize = sizeReferenced
	} else {
		progress.TotalSize = sizeWritten
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

	err = recv.Start()
	if err != nil {
		return karma.Format(
			err,
			"unable to start zfs recv",
		)
	}

	err = send.Start()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zfs send",
		)
	}

	progress.Sent = true

	progressFunc(progress)

	log.Info("{copy} send thread finished, now waiting for receive")

	err = recv.Wait()
	if err != nil {
		send.Process().Kill()

		return karma.Format(
			err,
			"unable to wait for zfs recv",
		)
	}

	err = send.Wait()
	if err != nil {
		return karma.Format(
			err,
			"unable to wait zfs send",
		)
	}

	return nil
}

func getSize(snapshot string) (uint64, uint64, error) {
	mappings, err := GetDatasetProperties([]PropertyRequest{
		{Name: constants.Written, System: true, Snapshot: true},
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
		sizeWritten    uint64
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
			case constants.Written:
				size, err := strconv.ParseUint(property.Value, 10, 64)
				if err != nil {
					return 0, 0, karma.
						Describe("size", property.Value).
						Format(
							err,
							"unable to parse written size",
						)
				}

				sizeWritten = size

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

	return sizeWritten, sizeReferenced, nil
}
