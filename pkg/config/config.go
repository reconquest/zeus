package config

import (
	"github.com/kovetskiy/ko"
)

type Config struct {
	Target     string `toml:"target" default:"zbackup"`
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

	return config, nil
}
