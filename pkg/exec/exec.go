package exec

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/kovetskiy/lorg"
	"github.com/reconquest/lexec-go"
)

var (
	logger = lorg.NewDiscarder().Tracef
)

func SetLogger(fn func(format string, values ...interface{})) {
	logger = fn
}

func Exec(command string, args ...string) *Execution {
	return &Execution{
		Execution: lexec.NewExec(
			getLogger(logger),
			exec.Command(command, args...),
		),

		stdout: nil,
		stderr: nil,
	}
}

func getLogger(fn func(format string, values ...interface{})) lexec.Logger {
	formatShellCommand := func(command []string) string {
		var (
			reSpecialChars       = regexp.MustCompile("[$`\"!']")
			reSpecialCharsEscape = regexp.MustCompile("[$`\"!]")
		)

		for i, arg := range command {
			if reSpecialChars.MatchString(arg) {
				command[i] = fmt.Sprintf(
					`"%s"`,
					reSpecialCharsEscape.ReplaceAllString(arg, `\&`),
				)
			}
		}

		return strings.Join(command, " ")
	}

	return func(command []string, stream lexec.Stream, data []byte) {
		if stream == lexec.InternalDebug {
			logger(
				`<%s> %s (%s) %s`,
				command[0],
				"command", formatShellCommand(command), string(data),
			)
		} else {
			logger(`<%s> %s: %s`, command[0], stream, string(data))
		}
	}
}
