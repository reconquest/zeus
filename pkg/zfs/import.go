package zfs

import (
	"regexp"
	"strings"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/exec"
)

func GetImportList() ([]string, error) {
	stdout, _, err := exec.Exec(`zpool`, `import`).Output()
	if err != nil {
		return nil, karma.Format(
			err,
			"unable to get availble pools for import",
		)
	}

	var (
		pools   = []string{}
		matches = regexp.MustCompile(`^\s+pool: ([\w :_.-]+)$`).
			FindAllStringSubmatch(strings.TrimSpace(stdout), -1)
	)

	for _, submatches := range matches {
		pools = append(pools, submatches[1])
	}

	return pools, nil
}

func GetImportedPools() ([]string, error) {
	stdout, _, err := exec.Exec(
		`zpool`, `list`, `-H`, `-o`, `name`,
	).Output()
	if err != nil {
		return nil, karma.Format(
			err,
			"unable to get imported pools",
		)
	}

	return strings.Split(strings.TrimSpace(stdout), "\n"), nil
}

func ImportPool(name string) error {
	err := exec.Exec(`zpool`, `import`, `-N`, name).Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zpool import",
		)
	}

	return nil
}
