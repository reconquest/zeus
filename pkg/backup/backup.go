package backup

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/reconquest/zeus/pkg/backup/errs"
	"github.com/reconquest/zeus/pkg/backup/housekeeping"
	"github.com/reconquest/zeus/pkg/backup/operation"
	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/constants"
	"github.com/reconquest/zeus/pkg/exec"
	pkg_log "github.com/reconquest/zeus/pkg/log"
	"github.com/reconquest/zeus/pkg/text"
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

type (
	Opts interface{}

	OptNoExport bool
)

func Backup(config *config.Config, opts ...Opts) error {
	var noExport bool

	for _, opt := range opts {
		switch opt := opt.(type) {
		case OptNoExport:
			noExport = bool(opt)
		}
	}

	targetDatasetName := fmt.Sprintf(
		"%s/%s",
		config.TargetPool,
		config.TargetDataset,
	)

	log.Infof("target backup pool: %q", config.TargetPool)
	log.Infof("target backup dataset: %q", targetDatasetName)

	log.Debugf("checking that target backup pool is imported")

	if ok, err := isPoolImported(config.TargetPool); !ok {
		if err != nil {
			return err
		}

		log.Infof(
			"target backup pool %q is not imported, importing pool",
			config.TargetPool,
		)

		err := zfs.ImportPool(config.TargetPool)
		if err != nil {
			return karma.Format(
				err,
				"unable to import pool",
			)
		}
	} else {
		log.Infof(
			"target backup %q pool is already imported",
			config.TargetPool,
		)
	}

	if !noExport {
		defer func() {
			log.Infof("exporting target backup pool %q", config.TargetPool)
			err := zfs.ExportPool(config.TargetPool)
			if err != nil {
				log.Errorf(karma.Format(
					err,
					"unable to export pool",
				).String())
			}
		}()
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

	log.Debugf("checking dataset %q encryption status", targetDatasetName)

	encrypted, encryptionRoot, err := loadEncryptionKey(
		config,
		config.TargetPool,
	)
	if err != nil {
		return karma.Format(
			err,
			"unable to load encryption key",
		)
	}

	err = zfs.EnsureDatasetExists(targetDatasetName)
	if err != nil {
		return karma.Format(
			err,
			"unable to create initial target dataset hierarchy",
		)
	}

	if encrypted {
		defer func() {
			log.Infof("unloading encryption key from %q", encryptionRoot)
			err := zfs.UnloadKey(encryptionRoot)
			if err != nil {
				log.Error(karma.Format(
					err,
					"unable to unload encryption key for %q",
					encryptionRoot,
				).String())
			}
		}()
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

	var backuped int

	currentSnapshot := config.SnapshotPrefix +
		time.Now().UTC().Format(time.RFC3339)

	for _, operation := range operations {
		var (
			namespace = "guid:" + operation.GUID[len(operation.GUID)-7:]
			source    = fmt.Sprintf("%s/%s", namespace, operation.Source)
		)

		operation.Target = fmt.Sprintf("%s/%s", targetDatasetName, namespace)

		operation.Snapshot.Current = currentSnapshot
		operation.Snapshot.Base = targetSnapshotsBySource[source]

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

		backuped++
	}

	log.Infof(
		"backup successfully completed for %d %s",
		backuped,
		text.Pluralize("dataset", backuped),
	)

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

func applyProperty(
	config *config.Config,
	operation BackupOperationWithHousekeeping,
	property zfs.Property,
) (BackupOperationWithHousekeeping, error) {
	switch property.Name {
	case constants.Backup:
		switch property.Value {
		case "on":
			operation.Enabled = true
			log.Infof(
				"will backup dataset %q",
				property.Source,
			)

		case "off":
			operation.Enabled = false

			log.Infof(
				"skipping dataset %q because property %q set to 'off'",
				property.Source, constants.Backup,
			)

		default:
			return operation, errs.UnsupportedPropertyValue(
				property,
				[]string{"on", "off"},
			)
		}

	case constants.GUID:
		operation.GUID = property.Value
	}

	return operation, nil
}

func getBackupOperations(
	config *config.Config,
) ([]BackupOperationWithHousekeeping, error) {
	mappings, err := zfs.GetDatasetProperties(
		append(
			[]zfs.PropertyRequest{
				{Name: constants.GUID, System: true, Filesystem: true},
				{Name: constants.Backup, Local: true, Filesystem: true},
			},
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
		}

		if !operation.Enabled {
			continue
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

func loadEncryptionKey(
	config *config.Config,
	dataset string,
) (bool, string, error) {
	mappings, err := zfs.GetDatasetProperties([]zfs.PropertyRequest{
		{Name: constants.Keystatus, System: true, Filesystem: true},
		{Name: constants.EncryptionRoot, System: true, Filesystem: true},
	}, dataset)
	if err != nil {
		return false, "", err
	}

	if len(mappings) == 0 {
		return false, "", nil
	}

	var encryptionRoot string
	var keyAlreadyLoaded bool

	for _, mapping := range mappings {
		if mapping.Source == dataset {
			for _, property := range mapping.Properties {
				switch property.Name {
				case constants.Keystatus:
					keyAlreadyLoaded =
						property.Value == constants.KeystatusAvailable
				case constants.EncryptionRoot:
					encryptionRoot = property.Value
				}
			}
		}
	}

	log.Infof("encryption detected on dataset %q", encryptionRoot)

	if keyAlreadyLoaded {
		return true, encryptionRoot, nil
	}

	log.Infof("loading encryption key for %q", encryptionRoot)

	var key string

	switch config.EncryptionKey.Provider {
	case "command":
		key, err = runEncryptionKeyProviderCommand(
			config.EncryptionKey.Command,
			encryptionRoot,
		)
		if err != nil {
			return false, "", err
		}
	default:
		return false, "", karma.
			Describe("provider", config.EncryptionKey.Provider).
			Format(
				err,
				"unsupported encryption key provider",
			)
	}

	err = zfs.LoadKey(encryptionRoot, key)
	if err != nil {
		return false, "", karma.Format(
			err,
			"unable to load encryption key to zfs",
		)
	}

	return true, encryptionRoot, nil
}

func runEncryptionKeyProviderCommand(
	config config.EncryptionKeyCommand,
	dataset string,
) (string, error) {
	facts := karma.
		Describe("executable", config.Executable).
		Describe("args", config.Args)

	args := make([]string, len(config.Args))

	copy(args, config.Args)

	for i, arg := range args {
		args[i] = strings.ReplaceAll(arg, "$DATASET", dataset)
	}

	execution := exec.Exec(config.Executable, args...)

	var stdout bytes.Buffer

	execution.SetStdout(&stdout)

	err := execution.Run()
	if err != nil {
		return "", facts.
			Format(
				err,
				"unable to run encryption key provider",
			)
	}

	key := stdout.String()

	if key == "" {
		return "", facts.Format(
			err,
			"provided encryption key is empty",
		)
	}

	return key, nil
}
