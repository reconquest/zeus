package zfs

import (
	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/exec"
)

func Hold(tag string, snapshot string) error {
	err := exec.Exec(`zfs`, `hold`, tag, snapshot).Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zfs hold",
		)
	}

	return nil
}

func Release(tag string, snapshot string) error {
	err := exec.Exec(`zfs`, `release`, tag, snapshot).Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zfs release",
		)
	}

	return nil
}
