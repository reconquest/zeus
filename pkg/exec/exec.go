package exec

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/kovetskiy/lorg"
	"github.com/reconquest/lexec-go"
)

var (
	logger  *lorg.Log
	counter int32
)

func SetLogger(log *lorg.Log) {
	logger = log
}

func Exec(command string, args ...string) *Execution {
	id := atomic.AddInt32(&counter, 1)

	return &Execution{
		Execution: lexec.NewExec(
			getLogger(
				logger.NewChildWithPrefix(
					fmt.Sprintf("<%s#%03d>", command, id),
				),
			),
			exec.Command(command, args...),
		),

		stdout: nil,
		stderr: nil,
	}
}

func getLogger(log *lorg.Log) lexec.Logger {
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
			log.Tracef(
				`%s (%s) %s`,
				"command", formatShellCommand(command), string(data),
			)
		} else {
			log.Tracef(
				`%s: %s`,
				stream, string(data),
			)
		}
	}
}
