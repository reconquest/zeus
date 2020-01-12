package housekeeping

import (
	"strings"

	"github.com/reconquest/zeus/pkg/backup/operation"
	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/constants"
	"github.com/reconquest/zeus/pkg/zfs"
)

type PolicyNone struct{}

func NewPolicyNone(*config.Config, []zfs.Property) (Policy, error) {
	return PolicyNone{}, nil
}

func (PolicyNone) GetName() string {
	return "none"
}

func (PolicyNone) Cleanup(operation operation.Backup) error {
	log.Warningf(
		strings.Join([]string{
			"housekeeping for source dataset %q is disabled",
			"No snapshots will be destroyed, so they will continue to pile up.",
			"You can enable simplest housekeeping for given dataset by using:",
			"  zfs set %s=%s %s",
		}, "\n"),
		operation.Source,
		constants.Housekeeping,
		PolicyByCount{}.GetName(),
		operation.Source,
	)

	return nil
}
