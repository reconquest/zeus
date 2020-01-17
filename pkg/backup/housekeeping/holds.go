package housekeeping

import (
	"fmt"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/backup/operation"
	"github.com/reconquest/zeus/pkg/zfs"
)

func ApplyHolds(tag string, operation operation.Backup) error {
	err := hold(tag, fmt.Sprintf(
		"%s@%s",
		operation.Source,
		operation.Snapshot.Current,
	))
	if err != nil {
		return err
	}

	err = hold(tag, fmt.Sprintf(
		"%s/%s@%s",
		operation.Target,
		operation.Source,
		operation.Snapshot.Current,
	))
	if err != nil {
		return err
	}

	if operation.Snapshot.Base == "" {
		return nil
	}

	err = release(tag, fmt.Sprintf(
		"%s/%s@%s",
		operation.Target,
		operation.Source,
		operation.Snapshot.Base,
	))
	if err != nil {
		return err
	}

	err = release(tag, fmt.Sprintf(
		"%s@%s",
		operation.Source,
		operation.Snapshot.Base,
	))
	if err != nil {
		return err
	}

	return nil
}

func hold(tag string, snapshot string) error {
	log.Debugf("putting hold on snapshot %q", snapshot)

	err := zfs.Hold(tag, snapshot)
	if err != nil {
		return karma.
			Describe("tag", tag).Format(
			err,
			"unable to put hold on snapshot %q",
			snapshot,
		)
	}

	return nil
}

func release(tag string, snapshot string) error {
	log.Debugf("releasing hold on snapshot %q", snapshot)

	held, err := zfs.HasHold(tag, snapshot)
	if err != nil {
		return karma.Format(
			err,
			"unable to check hold on snapshot %q",
			snapshot,
		)
	}

	if !held {
		return nil
	}

	err = zfs.Release(tag, snapshot)
	if err != nil {
		return karma.
			Describe("tag", tag).
			Format(
				err,
				"unable to release snapshot %q",
				snapshot,
			)
	}

	return nil
}
