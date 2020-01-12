package zfs

import (
	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/exec"
)

func EnsureDatasetExists(dataset string) error {
	err := exec.Exec(`zfs`, `create`, `-p`, dataset).Run()
	if err != nil {
		return karma.
			Describe("dataset", dataset).
			Format(
				err,
				"unable to create dataset hierarchy",
			)
	}

	return nil
}
