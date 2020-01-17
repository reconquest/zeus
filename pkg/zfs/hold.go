package zfs

import (
	"strings"

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

func ListHolds(snapshot string) ([]string, error) {
	stdout, _, err := exec.Exec(`zfs`, `holds`, `-H`, snapshot).Output()
	if err != nil {
		return nil, karma.Format(
			err,
			"unable to run zfs holds",
		)
	}

	tags := []string{}

	if len(stdout) == 0 {
		return nil, nil
	}

	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		fields := strings.SplitN(line, "\t", 3)
		if len(fields) != 3 {
			return nil, karma.
				Describe("line", line).
				Reason(
					"unexpected number of fields in zfs holds output",
				)
		}

		tags = append(tags, fields[1])
	}

	return tags, nil
}

func HasHold(tag string, snapshot string) (bool, error) {
	holds, err := ListHolds(snapshot)
	if err != nil {
		return false, err
	}

	for _, hold := range holds {
		if hold == tag {
			return true, nil
		}
	}

	return false, nil
}
