package housekeeping

import (
	"fmt"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/backup/operation"
	"github.com/reconquest/zeus/pkg/zfs"
)

func ApplyHolds(tag string, operation operation.Backup) error {
	facts := karma.Describe("tag", tag)

	sourceSnapshot := fmt.Sprintf(
		"%s@%s",
		operation.Source,
		operation.Snapshot.Current,
	)

	log.Debugf(
		"putting hold on current dataset snapshot %q",
		sourceSnapshot,
	)

	err := zfs.Hold(tag, sourceSnapshot)
	if err != nil {
		return facts.Format(
			err,
			"unable to put hold on source dataset snapshot %q",
			sourceSnapshot,
		)
	}

	targetSnapshot := fmt.Sprintf(
		"%s/%s@%s",
		operation.Target,
		operation.Source,
		operation.Snapshot.Current,
	)

	log.Debugf(
		"putting hold on backup dataset snapshot %q",
		targetSnapshot,
	)

	err = zfs.Hold(tag, targetSnapshot)
	if err != nil {
		return facts.Format(
			err,
			"unable to put hold on target dataset snapshot %q",
			targetSnapshot,
		)
	}

	if operation.Snapshot.Base == "" {
		return nil
	}

	targetBaseSnapshot := fmt.Sprintf(
		"%s/%s@%s",
		operation.Target,
		operation.Source,
		operation.Snapshot.Current,
	)

	log.Debugf(
		"releasing hold on base target snapshot %q",
		targetBaseSnapshot,
	)

	err = zfs.Release(tag, targetBaseSnapshot)
	if err != nil {
		return facts.
			Format(
				err,
				"unable to release target base snapshot %q",
				operation.Snapshot.Current,
			)
	}

	sourceBaseSnapshot := fmt.Sprintf(
		"%s@%s",
		operation.Source,
		operation.Snapshot.Current,
	)

	log.Debugf(
		"releasing hold on base source snapshot %q",
		sourceBaseSnapshot,
	)

	err = zfs.Release(tag, sourceBaseSnapshot)
	if err != nil {
		return facts.
			Format(
				err,
				"unable to release source base snapshot %q",
				operation.Snapshot.Current,
			)
	}

	return nil
}
