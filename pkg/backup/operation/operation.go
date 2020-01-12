package operation

import (
	"fmt"
	"time"

	"github.com/kovetskiy/lorg"
	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/formatting"
	pkg_log "github.com/reconquest/zeus/pkg/log"
	"github.com/reconquest/zeus/pkg/zfs"
)

type (
	Backup struct {
		Enabled bool
		Source  string
		Target  string

		Snapshot struct {
			Current string
			Base    string
		}
	}
)

var (
	log = pkg_log.NewChildWithPrefix("{backup}")
)

func (operation Backup) Run() error {
	err := zfs.EnsureDatasetExists(
		fmt.Sprintf("%s/%s", operation.Target, operation.Source),
	)
	if err != nil {
		return karma.Format(
			err,
			"unable to create source dataset hierarchy on target",
		)
	}

	log.Debugf(
		"creating snapshot %q on dataset %q",
		operation.Snapshot.Current,
		operation.Source,
	)

	sourceSnapshot := fmt.Sprintf(
		"%s@%s",
		operation.Source,
		operation.Snapshot.Current,
	)

	err = zfs.CreateSnapshot(sourceSnapshot)
	if err != nil {
		return err
	}

	if operation.Snapshot.Base != "" {
		log.Infof(
			"starting incremental send: %q..%q -> %q",
			fmt.Sprintf("%s@%s", operation.Source, operation.Snapshot.Base),
			fmt.Sprintf("%s@%s", operation.Source, operation.Snapshot.Current),
			operation.Target,
		)

		operation.Snapshot.Base = fmt.Sprintf(
			"%s@%s",
			operation.Source,
			operation.Snapshot.Base,
		)
	} else {
		log.Infof(
			"starting full send: %q -> %q",
			fmt.Sprintf("%s@%s", operation.Source, operation.Snapshot.Base),
			operation.Target,
		)
	}

	err = zfs.CopyDataset(
		sourceSnapshot,
		operation.Target,
		operation.Snapshot.Base,
		createCopyProgressLogger(
			log.NewChildWithPrefix(
				fmt.Sprintf("sending %s:", sourceSnapshot),
			),
		),
	)
	if err != nil {
		return karma.
			Describe("source", sourceSnapshot).
			Describe("target", operation.Target).
			Format(
				err,
				"unable to perform dataset copy",
			)
	}

	return nil
}

func createCopyProgressLogger(log *lorg.Log) func(progress zfs.CopyProgress) {
	return func(progress zfs.CopyProgress) {
		running := progress.Updated.Sub(progress.Started)

		if progress.SentSize > 0 && running.Seconds() > 0 {
			if progress.SentSize == progress.EstimatedSize {
				log.Infof(
					"complete in %s | %s %s",
					running.Round(time.Millisecond),
					progress.SendType,
					formatting.Size(progress.SentSize),
				)

				log.Infof("now waiting receive to finish")

				return
			}

			rate := float64(progress.SentSize) / running.Seconds()
			var left float64

			if progress.EstimatedSize >= progress.SentSize {
				left = float64(progress.EstimatedSize-progress.SentSize) / rate
			} else {
				left = 0
			}

			log.Debugf(
				"%-8s | %s %s | eta %s",
				formatting.Size(progress.SentSize),
				progress.SendType,
				formatting.Size(progress.EstimatedSize),
				time.Duration(time.Duration(left)*time.Second),
			)
		}

	}
}
