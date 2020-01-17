package operation

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/kovetskiy/lorg"
	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/constants"
	"github.com/reconquest/zeus/pkg/formatting"
	pkg_log "github.com/reconquest/zeus/pkg/log"
	"github.com/reconquest/zeus/pkg/zfs"
)

type (
	Backup struct {
		Enabled bool
		GUID    string
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
	err := operation.ensureTargetDataset()
	if err != nil {
		return err
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

	err = zfs.SetDatasetProperty(sourceSnapshot, constants.Managed, `yes`)
	if err != nil {
		return karma.Format(
			err,
			"unable to set managed mark on source snapshot",
		)
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
			fmt.Sprintf("%s@%s", operation.Source, operation.Snapshot.Current),
			operation.Target,
		)
	}

	err = zfs.CopyDataset(
		sourceSnapshot,
		operation.Target,
		operation.Snapshot.Base,
		createCopyProgressLogger(
			log.NewChildWithPrefix(
				fmt.Sprintf("{zfs send} sending %s:", sourceSnapshot),
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

	err = zfs.SetDatasetProperty(
		fmt.Sprintf("%s/%s", operation.Target, sourceSnapshot),
		constants.Managed,
		`yes`,
	)
	if err != nil {
		return karma.Format(
			err,
			"unabled to set managed mark on target snapshot",
		)
	}

	return nil
}

func (operation *Backup) ensureTargetDataset() error {
	parent := filepath.Dir(
		fmt.Sprintf("%s/%s", operation.Target, operation.Source),
	)

	log.Debugf("ensuring required parent dataset %q", parent)

	err := zfs.EnsureDatasetExists(parent)
	if err != nil {
		return karma.Format(
			err,
			"unable to create source dataset hierarchy on target",
		)
	}

	return nil
}

func createCopyProgressLogger(log *lorg.Log) func(progress zfs.CopyProgress) {
	return func(progress zfs.CopyProgress) {
		if !progress.Sent {
			running := time.Now().Sub(progress.StartedAt)

			rate := float64(progress.SentSize) / running.Seconds()
			var left float64

			if progress.SentSize <= progress.TotalSize {
				left = float64(progress.TotalSize-progress.SentSize) / rate
			} else {
				left = 0
			}

			log.Debugf(
				"%-8s / %-8s | eta %s",
				formatting.Size(progress.SentSize),
				formatting.Size(progress.TotalSize),
				time.Duration(time.Duration(left)*time.Second),
			)
		}
	}
}
