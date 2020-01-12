package zfs

import (
	"strings"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/exec"
)

func CreateSnapshot(snapshot string) error {
	err := exec.Exec(`zfs`, `snapshot`, snapshot).Run()
	if err != nil {
		return karma.
			Describe("snapshot", snapshot).
			Format(
				err,
				"unable to create dataset snapshot",
			)
	}

	return nil
}

func ListSnapshots(dataset string) ([]string, error) {
	stdout, _, err := exec.Exec(
		`zfs`, `list`, `-t`, `snap`, `-Hro`, `name`, dataset,
	).Output()
	if err != nil {
		return nil, karma.
			Describe("dataset", dataset).
			Format(
				err,
				"unable to list snapshots",
			)
	}

	snapshots := strings.Split(strings.TrimSpace(stdout), "\n")

	return snapshots, nil
}

func SplitSnapshotName(name string) (string, string, error) {
	parts := strings.SplitN(name, "@", 2)
	if len(parts) != 2 {
		return "", "", karma.
			Describe("name", name).
			Reason(
				`internal error: snapshot name doesn't contain '@'`,
			)
	}

	return parts[0], parts[1], nil
}
