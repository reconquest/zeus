package zfs

import (
	"bytes"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/exec"
)

func LoadKey(dataset string, key string) error {
	execution := exec.Exec(`zfs`, `load-key`, dataset)

	execution.SetStdin(bytes.NewBufferString(key))

	err := execution.Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zfs load-key",
		)
	}

	return nil
}

func UnloadKey(dataset string) error {
	execution := exec.Exec(`zfs`, `unload-key`, dataset)

	err := execution.Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zfs unload-key",
		)
	}

	return nil
}
