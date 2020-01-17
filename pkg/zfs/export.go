package zfs

import (
	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/exec"
)

func ExportPool(name string) error {
	err := exec.Exec(`zpool`, `export`, name).Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zpool export",
		)
	}

	return nil
}
