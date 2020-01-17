package config

import (
	"os"

	"github.com/kovetskiy/ko"
	"github.com/reconquest/karma-go"
)

type Config struct {
	TargetPool    string `toml:"target_pool" default:"zbackup"`
	TargetDataset string `toml:"target_dataset" default:"$HOSTNAME"`

	SnapshotPrefix string `toml:"snapshot_prefix" default:"'zeus:'"`

	HoldTag string `toml:"hold_tag" default:"zeus" required:"true"`

	EncryptionKey struct {
		Provider string `toml:"provider" default:"command"`

		Command EncryptionKeyCommand `toml:"command"`
	} `toml:"encryption_key"`

	Defaults struct {
		Housekeeping struct {
			Policy string `toml:"policy" default:"by-count"`

			ByCount struct {
				KeepOnTarget int `toml:"keep_on_target" default:"10"`
				KeepOnSource int `toml:"keep_on_source" default:"1"`
			} `toml:"by_count"`
		} `toml:"housekeeping"`
	} `toml:"defaults"`
}

type EncryptionKeyCommand struct {
	Executable string   `toml:"executable" default:"zfs-encryption-key"`
	Args       []string `toml:"args" default:"['$DATASET']"`
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
