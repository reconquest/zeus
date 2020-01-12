package backup

import (
	"fmt"
	"strings"
	"time"

	"github.com/reconquest/zeus/pkg/backup/housekeeping"
	"github.com/reconquest/zeus/pkg/backup/operation"
	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/constants"
	pkg_log "github.com/reconquest/zeus/pkg/log"
	"github.com/reconquest/zeus/pkg/zfs"

	"github.com/reconquest/karma-go"
)

var (
	log = pkg_log.NewChildWithPrefix("{backup}")
)

type (
	BackupOperationWithHousekeeping struct {
		operation.Backup
		housekeeping.Policy
	}
)

func Backup(config *config.Config) error {
	targetDatasetName := fmt.Sprintf(
		"%s/%s",
		config.TargetPool,
		config.TargetDataset,
	)

	log.Infof("target backup pool: %q", config.TargetPool)
	log.Infof("target backup dataset: %q", targetDatasetName)

	log.Debugf("checking that backup pool is imported")

	if ok, err := isPoolImported(config.TargetPool); !ok {
		if err != nil {
			return err
		}

		log.Debugf("backup pool is not imported, importing pool")

		err := zfs.ImportPool(config.TargetPool)
		if err != nil {
			return karma.Format(
				err,
				"unable to import pool",
			)
		}
	} else {
		log.Debugf("backup pool is already imported")
	}

	// TODO(seletskiy): check S.M.A.R.T. before attempting backup

	log.Debugf("retrieving datasets to backup")

	operations, err := getBackupOperations(config)
	if err != nil {
		return karma.Format(
			err,
			"unable to retrieve datasets for backup",
		)
	}

	if len(operations) == 0 {
		log.Warningf(
			strings.Join(
				[]string{
					"no zfs datasets marked for backup",
					"Set '%[1]s' property to enable backup for any dataset like this:",
					"  zfs set %[1]s=on <your-dataset>",
				},
				"\n",
			),
			constants.Backup,
		)

		return nil
	}

	err = zfs.EnsureDatasetExists(targetDatasetName)
	if err != nil {
		return karma.Format(
			err,
			"unable to create source dataset hierarchy on target",
		)
	}

	log.Debugf("listing snapshots on the backup dataset %q", targetDatasetName)

	targetSnapshotsBySource, err := getLatestTargetSnapshotsBySource(
		targetDatasetName,
	)
	if err != nil {
		return karma.Format(
			err,
			"unable to retrieve exising snapshots in backup dataset",
		)
	}

	currentSnapshot := config.SnapshotPrefix +
		time.Now().UTC().Format(time.RFC3339)

	for _, operation := range operations {
		operation.Target = targetDatasetName
		operation.Snapshot.Current = currentSnapshot
		operation.Snapshot.Base = targetSnapshotsBySource[operation.Source]

		err := operation.Run()
		if err != nil {
			return err
		}

		log.Infof(
			"dataset copy %q -> %q completed successfully",
			fmt.Sprintf("%s@%s", operation.Source, operation.Snapshot.Current),
			operation.Target,
		)

		err = housekeeping.ApplyHolds(config.HoldTag, operation.Backup)
		if err != nil {
			return karma.Format(
				err,
				"unable to apply holds",
			)
		}

		log.Infof(
			"applying houseskeeping policy %q | source %q | target %q",
			operation.Policy.GetName(),
			operation.Source,
			operation.Target,
		)

		err = operation.Policy.Cleanup(operation.Backup)
		if err != nil {
			return karma.Format(
				err,
				"unable to run housekeeping",
			)
		}
	}

	return nil
}

func isPoolImported(name string) (bool, error) {
	if ok, err := isPoolInImportList(name); ok {
		return true, nil
	} else {
		if err != nil {
			return false, karma.Format(
				err,
				"unable to check that pool is in import list",
			)
		}

		if ok, err := isPoolInImportedList(name); ok {
			return true, nil
		} else {
			if err != nil {
				return false, karma.Format(
					err,
					"unable to check that pool is in imported list",
				)
			}

			return false, nil
		}
	}
}

func isPoolInImportList(name string) (bool, error) {
	pools, err := zfs.GetImportList()
	if err != nil {
		return false, err
	}

	for _, pool := range pools {
		if pool == name {
			return true, nil
		}
	}

	return false, nil
}

func isPoolInImportedList(name string) (bool, error) {
	importedPools, err := zfs.GetImportedPools()
	if err != nil {
		return false, err
	}

	for _, importedPoolName := range importedPools {
		if importedPoolName == name {
			return true, nil
		}
	}

	return false, nil
}

func errUnsupportedPropertyValue(
	givenValue string,
	supportedValues []string,
) error {
	return karma.
		Describe("value", givenValue).
		Reason(
			fmt.Errorf(
				strings.Join(
					[]string{
						"unsuported value given for property",
						"supported values are: %q",
					},
					"\n",
				),
				supportedValues,
			),
		)
}

func parseBackupProperty(value string) (bool, error) {
	switch value {
	case "on":
		return true, nil

	case "off":
		return false, nil

	default:
		return false, errUnsupportedPropertyValue(
			value,
			[]string{"on", "off"},
		)
	}
}

func parseHousekeepingProperty(value string) (string, error) {
	switch value {
	case "none", "by-count":
		return value, nil

	default:
		return "", errUnsupportedPropertyValue(
			value,
			[]string{"none", "by-count"},
		)
	}
}

func applyProperty(
	config *config.Config,
	operation BackupOperationWithHousekeeping,
	property zfs.Property,
) (BackupOperationWithHousekeeping, error) {
	facts := karma.
		Describe("property", property.Name).
		Describe("dataset", property.Source)

	switch property.Name {
	case constants.Backup:
		enabled, err := parseBackupProperty(property.Value)
		if err != nil {
			return operation, facts.Reason(err)
		}

		operation.Enabled = enabled
	}

	return operation, nil
}

func getBackupOperations(
	config *config.Config,
) ([]BackupOperationWithHousekeeping, error) {
	mappings, err := zfs.GetDatasetProperties(
		append(
			[]zfs.PropertyRequest{{Name: constants.Backup}},
			housekeeping.Properties...,
		),
	)
	if err != nil {
		return nil, karma.Format(
			err,
			"unable to retrieve datasets for backup",
		)
	}

	var (
		operations = []BackupOperationWithHousekeeping{}
	)

mappingsLoop:
	for _, mapping := range mappings {
		var operation BackupOperationWithHousekeeping

		operation.Source = mapping.Source

		for _, property := range mapping.Properties {
			operation, err = applyProperty(config, operation, property)
			if err != nil {
				log.Warning(err)

				continue mappingsLoop
			}

			if !operation.Enabled {
				log.Infof(
					"skipping operation %q because property %q set to 'off'",
					property.Source, constants.Backup,
				)

				continue mappingsLoop
			}
		}

		policy, err := housekeeping.Configure(config, mapping.Properties)
		if err != nil {
			return nil, karma.Format(
				err,
				"unable to configure housekeeping policy",
			)
		}

		operation.Policy = policy

		operations = append(operations, operation)
	}

	return operations, nil
}

func getLatestTargetSnapshotsBySource(
	targetDatasetName string,
) (map[string]string, error) {
	targetSnapshots, err := zfs.ListSnapshots(targetDatasetName)
	if err != nil {
		return nil, err
	}

	mapping := map[string]string{}
	for _, targetSnapshot := range targetSnapshots {
		parts := strings.SplitN(
			strings.TrimPrefix(targetSnapshot, targetDatasetName),
			"@",
			2,
		)

		if len(parts) != 2 {
			return nil, karma.
				Describe("snapshot", targetSnapshot).
				Format(
					err,
					"internal error: snapshot doesn't contain '@'",
				)
		}

		mapping[strings.TrimPrefix(parts[0], "/")] = parts[1]
	}

	return mapping, nil
}
