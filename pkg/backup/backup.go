package backup

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/reconquest/callbackwriter-go"
	"github.com/reconquest/lineflushwriter-go"
	"github.com/reconquest/nopio-go"
	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/exec"
	"github.com/reconquest/zeus/pkg/log"

	"github.com/reconquest/karma-go"
)

type (
	datasetProperty struct {
		Dataset string
		Value   string
	}
)

func Backup(config *config.Config) error {
	targetDataset := fmt.Sprintf(
		"%s/%s",
		config.TargetPool,
		config.TargetDataset,
	)

	log.Infof("target backup pool: %q", config.TargetPool)
	log.Infof("target backup dataset: %q", targetDataset)

	log.Debugf("checking that backup pool is imported")

	if ok, err := isPoolImported(config.TargetPool); !ok {
		if err != nil {
			return err
		}

		log.Debugf("backup pool is not imported, importing pool")

		err := importPool(config.TargetPool)
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

	datasets, err := getDatasetsForBackup(config)
	if err != nil {
		return karma.Format(
			err,
			"unable to retrieve datasets for backup",
		)
	}

	log.Debugf("listing snapshot on the backup dataset")

	existingTargetSnapshots, err := getTargetSnapshotMappingToSourceDatasets(
		targetDataset,
	)
	if err != nil {
		return karma.Format(
			err,
			"unable to retrieve exising snapshots in backup dataset",
		)
	}

	snapshotName := time.Now().UTC().Format(time.RFC3339)

	log.Debugf("current backup snapshot name: %q", snapshotName)

	for _, dataset := range datasets {
		err = ensureDatasetHierarchy(dataset, targetDataset)
		if err != nil {
			return karma.Format(
				err,
				"unable to create source dataset hierarchy on target",
			)
		}

		log.Debugf("creating snapshot %q on dataset %q", dataset, snapshotName)

		fullSnapshotName, err := snapshot(dataset, snapshotName)
		if err != nil {
			return err
		}

		log.Infof("starting dataset copy: %q -> %q", fullSnapshotName, targetDataset)

		err = copyDataset(
			fullSnapshotName,
			targetDataset,
			existingTargetSnapshots[dataset],
		)
		if err != nil {
			return karma.
				Describe("source", fullSnapshotName).
				Describe("target", targetDataset).
				Format(
					err,
					"unable to perform dataset copy",
				)
		}
	}

	//for _, sourceName := range sources {
	//    err := copyPool(sourceName, config.Target)
	//    if err != nil {
	//        return karma.Format(
	//            err,
	//            "unable to copy pool %s to %s",
	//            sourceName,
	//            config.Target,
	//        )
	//    }
	//}

	//log.Infof("backuping pools: %s", strings.Join(sources, ", "))

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
	stdout, _, err := exec.Exec(`zpool`, `import`).Output()
	if err != nil {
		return false, karma.Format(
			err,
			"unable to get availble pools for import",
		)
	}

	if regexp.MustCompile(`^\s+pool: ` + name).MatchString(stdout) {
		return false, nil
	}

	return false, nil
}

func getImportedPools() ([]string, error) {
	stdout, _, err := exec.Exec(
		`zpool`, `list`, `-H`, `-o`, `name`,
	).Cached().Output()
	if err != nil {
		return nil, karma.Format(
			err,
			"unable to get imported pools",
		)
	}

	return strings.Split(strings.TrimSpace(stdout), "\n"), nil
}

func isPoolInImportedList(name string) (bool, error) {
	importedPools, err := getImportedPools()
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

func importPool(name string) error {
	err := exec.Exec(`zpool`, `import`, `-N`, name).Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to run zpool import",
		)
	}

	return nil
}

func getDatasetsForBackup(config *config.Config) ([]string, error) {
	properties, err := getDatasetProperty(config.Properties.Backup)
	if err != nil {
		return nil, karma.Format(
			err,
			"unable to retrieve datasets for backup",
		)
	}

	if len(properties) == 0 {
		log.Warningf(
			strings.Join(
				[]string{
					"no zfs datasets marked for backup",
					"set '%[1]s' property to enable backup for any dataset like this:",
					"  zfs set %[1]s=on <your-dataset>",
				},
				"\n",
			),
			config.Properties.Backup,
		)

		return nil, nil
	}

	var datasets []string

	for _, property := range properties {
		switch property.Value {
		case "on":
			datasets = append(datasets, property.Dataset)

		case "off":
			log.Infof(
				"skipping dataset %q because property %q set to 'off'",
				property.Dataset, config.Properties.Backup,
			)

		default:
			log.Warningf(
				strings.Join(
					[]string{
						"unsuported value for property %q on dataset %q",
						"supported values are: 'on', 'off'",
					},
					"\n",
				),
			)
		}
	}

	return datasets, nil
}

func getDatasetProperty(name string) ([]datasetProperty, error) {
	command := exec.Exec(
		`zfs`, `get`, name, `-H`, `-o`, `name,value`, `-s`, `local`,
	)

	stdout, _, err := command.Output()
	if err != nil {
		return nil, karma.
			Describe("property", name).
			Format(
				err,
				"unable to list property from zfs datasets",
			)
	}

	var properties []datasetProperty

	for _, mapping := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if mapping == "" {
			break
		}

		fields := strings.SplitN(mapping, "\t", 2)
		if len(fields) != 2 {
			return nil, karma.
				Describe("command", command).
				Describe("mapping", mapping).
				Format(
					err,
					"unexpected number of fields in property get response",
				)
		}

		if fields[1] == "-" {
			continue
		}

		properties = append(properties, datasetProperty{
			Dataset: fields[0],
			Value:   fields[1],
		})
	}

	return properties, nil
}

func snapshot(dataset string, name string) (string, error) {
	fullName := fmt.Sprintf(`%s@%s`, dataset, name)

	err := exec.Exec(`zfs`, `snapshot`, fullName).Run()
	if err != nil {
		return "", karma.
			Describe("snapshot", fullName).
			Format(
				err,
				"unable to create dataset snapshot",
			)
	}

	return fullName, nil
}

func listSnapshots(dataset string) ([]string, error) {
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

func getTargetSnapshotMappingToSourceDatasets(
	targetDataset string,
) (map[string]string, error) {
	snapshots, err := listSnapshots(targetDataset)
	if err != nil {
		return nil, err
	}

	mapping := map[string]string{}
	for _, snapshot := range snapshots {
		sourceDataset := strings.TrimPrefix(
			strings.SplitN(
				strings.TrimPrefix(snapshot, targetDataset),
				"@",
				2,
			)[0],
			"/",
		)

		mapping[sourceDataset] = snapshot
	}

	return mapping, nil
}

func copyDataset(
	sourceSnapshot string,
	targetDataset string,
	targetSnapshot string,
) error {
	sendArgs := []string{
		`send`, `-Pvc`, sourceSnapshot,
	}

	if targetSnapshot != "" {
		if !strings.Contains(targetSnapshot, "@") {
			return karma.
				Describe("snapshot", targetSnapshot).
				Reason("internal error: snapshot name doen't contain '@'")
		}

		baseSnapshot := fmt.Sprintf(
			"%s@%s",
			strings.SplitN(sourceSnapshot, "@", 2)[0],
			strings.SplitN(targetSnapshot, "@", 2)[1],
		)

		log.Debugf(
			"starting incremental send: %q..%q -> %q",
			baseSnapshot,
			sourceSnapshot,
			targetDataset,
		)

		sendArgs = append(sendArgs, `-i`, baseSnapshot)
	} else {
		log.Debugf(
			"starting full send: %q -> %q",
			sourceSnapshot,
			targetDataset,
		)
	}

	var (
		recv = exec.Exec(
			`zfs`, `recv`, `-F`, fmt.Sprintf(
				"%s/%s",
				targetDataset,
				sourceSnapshot,
			),
		)

		send = exec.Exec(`zfs`, sendArgs...).NoStdLog()
	)

	logger := lineflushwriter.New(
		callbackwriter.New(
			nopio.NopWriteCloser{},
			func(line []byte) {
				log.Debugf(
					"{zfs send} %s -> %s: %s",
					sourceSnapshot,
					targetDataset,
					bytes.Trim(line, "\n"),
				)
			},
			nil,
		),
		&sync.Mutex{},
		true,
	)

	stdout, err := send.StdoutPipe()
	if err != nil {
		return err
	}

	send.SetStderr(logger)
	recv.SetStdin(stdout)

	err = recv.Start()
	if err != nil {
		return karma.Format(
			err,
			"unable to start receive command",
		)
	}

	err = send.Run()
	if err != nil {
		return karma.Format(
			err,
			"unable to execute run command",
		)
	}

	err = recv.Wait()
	if err != nil {
		return karma.Format(
			err,
			"unable to finish execution of receive command",
		)
	}

	return nil
}

func ensureDatasetHierarchy(sourceDataset string, targetDataset string) error {
	dataset := fmt.Sprintf("%s/%s", targetDataset, sourceDataset)

	err := exec.Exec(`zfs`, `create`, `-p`, dataset).Run()
	if err != nil {
		return karma.
			Describe("dataset", targetDataset).
			Format(
				err,
				"unable to create pivot dataset in target dataset",
			)
	}

	return nil
}
