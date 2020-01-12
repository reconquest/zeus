package housekeeping

import (
	"github.com/reconquest/zeus/pkg/backup/operation"
	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/zfs"
)

type PolicyConstructor = func(*config.Config, []zfs.Property) (Policy, error)

type Policy interface {
	Cleanup(operation.Backup) error
	GetName() string
}
