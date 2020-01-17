package main

import (
	"os"
	"os/user"
	"path/filepath"

	"github.com/docopt/docopt-go"
	"github.com/kovetskiy/lorg"

	"github.com/reconquest/zeus/pkg/backup"
	"github.com/reconquest/zeus/pkg/config"
	"github.com/reconquest/zeus/pkg/exec"
	"github.com/reconquest/zeus/pkg/log"
)

var version = "[manual build]"

var usage = `zeus - zfs backup tool.

Usage:
  zeus -h | --help
  zeus [options] backup [--no-export]

Options:
  -h --help           Show this help.
  -c --config=<file>  Specify config file.
                       [default: $CONFIG]
  --no-export         Will not export target pool at the end of backup
                       operation.
  --debug             Output debug messages in logs.
  --trace             Output trace messages in logs.
`

type Opts struct {
	ValueConfig  string `docopt:"--config"`
	ModeBackup   bool   `docopt:"backup"`
	FlagNoExport bool   `docopt:"--no-export"`
	FlagDebug    bool   `docopt:"--debug"`
	FlagTrace    bool   `docopt:"--trace"`
}

func init() {
	user, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}

	env := func(key, defaultValue string) {
		if os.Getenv(key) == "" {
			os.Setenv(key, defaultValue)
		}
	}

	var config string

	if user.Uid == "0" {
		config = "/etc/zeus/zeusd.conf"
	} else {
		config = filepath.Join(user.HomeDir, ".config", "zeus", "zeusd.conf")
	}

	env("CONFIG", config)

	usage = os.ExpandEnv(usage)
}

func main() {
	args, err := docopt.ParseArgs(usage, nil, "zeus "+version)
	if err != nil {
		log.Fatal(err)
	}

	var opts Opts

	err = args.Bind(&opts)
	if err != nil {
		log.Fatal(err)
	}

	switch {
	case opts.FlagDebug:
		log.SetLevel(lorg.LevelDebug)
	case opts.FlagTrace:
		log.SetLevel(lorg.LevelTrace)
	}

	exec.SetLogger(log.NewChildWithPrefix("{exec}"))

	config, err := config.LoadConfig(opts.ValueConfig)
	if err != nil {
		log.Fatal(err)
	}

	switch {
	case opts.ModeBackup:
		err = backup.Backup(
			config,
			backup.OptNoExport(opts.FlagNoExport),
		)
	}

	if err != nil {
		log.Fatal(err)
	}
}
