package backup

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/exec"
	"github.com/reconquest/zeus/pkg/log"

	"github.com/reconquest/karma-go"
)

func Backup(config *config.Config) error {
	log := log.NewChildWithPrefix(fmt.Sprintf("{pool %s}", config.Target))

	log.Debugf("checking that backup pool is imported")

	if ok, err := isPoolImported(config.Target); !ok {
		if err != nil {
			return err
		}

		log.Debugf("backup pool is not imported, importing pool")

		err := importPool(config.Target)
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

	log.Debugf("retrieving filesystems to backup")

	fs, err := getZFSProperty(config.Properties.Backup)
	if err != nil {
		return karma.Format(
			err,
			"unable to retrieve filesystems for backup",
		)
	}

	if len(fs) == 0 {
		log.Warningf(
			strings.Join(
				[]string{
					"no zfs filesystems marked for backup",
					"set '%[1]s' property to enable backup for any filesystem like this:",
					"  zfs set %[1]s=on <your-filesystem>",
				},
				"\n",
			),
			config.Properties.Backup,
		)
		return nil
	}

	snapshotName := time.Now().UTC().Format(time.RFC3339)

	for fsName, propValue := range fs {
		switch propValue {
		case "on":
			// ok
		case "off":
			log.Infof(
				"skipping fs %s because property '%s' set to 'off'",
				fsName, config.Properties.Backup,
			)
			continue
		default:
			log.Warningf(
				strings.Join(
					[]string{
						"unsuported value for property '%s' on filesystem %s",
						"supported values are: 'on', 'off'",
					},
					"\n",
				),
			)

		}

		fullSnapshotName, err := snapshot(fsName, snapshotName)
		if err != nil {
			return err
		}

		err = copyFilesystem(fullSnapshotName, config.Target)
		if err != nil {
			return karma.Format(
				err,
				"unable to perform filesystem copy %s -> %s",
				fullSnapshotName,
				config.Target,
			)
		}
	}

	fmt.Fprintf(os.Stderr, "XXXXXX backup.go:47 fs: %#v\n", fs)

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

func getZFSProperty(name string) (map[string]string, error) {
	command := exec.Exec(
		`zfs`, `get`, name, `-H`, `-o`, `name,value`, `-s`, `local`,
	)

	stdout, _, err := command.Output()
	if err != nil {
		return nil, karma.
			Describe("property", name).
			Format(
				err,
				"unable to list property from zfs filesystems",
			)
	}

	properties := map[string]string{}

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

		properties[fields[0]] = fields[1]
	}

	return properties, nil
}

func snapshot(fs string, name string) (string, error) {
	fullName := fmt.Sprintf(`%s@%s`, fs, name)
	err := exec.Exec(`zfs`, `snapshot`, fullName).Run()
	if err != nil {
		return "", karma.Format(
			err,
			"unable to create filesystem snapshot %s",
			fullName,
		)
	}

	return fullName, nil
}

func copyFilesystem(sourceSnapshot string, targetPool string) error {
	var (
		send = exec.Exec(
			`zfs`, `send`, `-Pvc`, sourceSnapshot,
		).NoStdLog()

		recv = exec.Exec(
			`zfs`, `recv`, `-F`, fmt.Sprintf(
				"%s/%s",
				targetPool,
				sourceSnapshot,
			),
		)
	)

	stdout, err := send.StdoutPipe()
	if err != nil {
		return err
	}

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
