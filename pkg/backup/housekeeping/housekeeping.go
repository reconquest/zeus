package housekeeping

import (
	"github.com/reconquest/zeus/pkg/backup/errs"
	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/constants"
	pkg_log "github.com/reconquest/zeus/pkg/log"
	"github.com/reconquest/zeus/pkg/zfs"
)

var (
	Properties = []zfs.PropertyRequest{
		{Name: constants.Housekeeping, Inherited: true},
		// more properties will be appended by various housekeeping policies
	}
)

var (
	log = pkg_log.NewChildWithPrefix(`{housekeeping}`)
)

func Configure(
	config *config.Config,
	properties []zfs.Property,
) (Policy, error) {
	var property *zfs.Property
	var constructor PolicyConstructor

	for _, candidate := range properties {
		if candidate.Name == constants.Housekeeping {
			property = &candidate
			break
		}
	}

	if property == nil {
		property = &zfs.Property{
			Value: config.Defaults.Housekeeping.Policy,
		}
	}

	switch property.Value {
	case PolicyNone{}.GetName():
		constructor = NewPolicyNone

	case PolicyByCount{}.GetName():
		constructor = NewPolicyByCount

	default:
		return nil, errs.UnsupportedPropertyValue(
			*property,
			[]string{
				PolicyNone{}.GetName(),
				PolicyByCount{}.GetName(),
			},
		)
	}

	policy, err := constructor(config, properties)
	if err != nil {
		return nil, err
	}

	return policy, nil
}
