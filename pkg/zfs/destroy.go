package zfs

import (
	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/exec"
)

func DestroyDataset(name string) error {
	err := exec.Exec(`zfs`, `destroy`, name).Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zfs destroy",
		)
	}

	return nil
}
