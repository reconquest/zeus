package config

import (
	"os"

	"github.com/kovetskiy/ko"
	"github.com/reconquest/karma-go"
)

type Config struct {
	TargetPool    string `toml:"target_pool" default:"zbackup"`
	TargetDataset string `toml:"target_dataset" default:"$HOSTNAME"`

	Properties struct {
		Backup         string `toml:"backup" default:"zeus:backup"`
		BackupInterval string `toml:"backup_interval" default:"zeus:backup-interval"`
	} `toml:"properties"`
}

func LoadConfig(path string) (*Config, error) {
	config := &Config{}
	err := ko.Load(path, config, ko.RequireFile(false))
	if err != nil {
		return nil, err
	}

	if config.TargetDataset == "$HOSTNAME" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, karma.Format(
				err,
				"unable to retrieve current hostname to use as default target dataset name",
			)
		}

		config.TargetDataset = hostname
	}

	return config, nil
}
