package exec

import (
	"io"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/lexec-go"
)

type Execution struct {
	*lexec.Execution

	stdout io.Writer
	stderr io.Writer
}

func (execution *Execution) Cached() *ExecutionCached {
	return &ExecutionCached{
		Execution: execution,
	}
}

func (execution *Execution) Output() (string, string, error) {
	stdout, stderr, err := execution.Execution.Output()
	if err != nil {
		return string(stdout), string(stderr), karma.Format(
			err,
			"unable to run command",
		)
	}

	return string(stdout), string(stderr), nil
}

//func (execution *Execution) Run() error {
//    err := execution.Execution.Run()
//    if err != nil {
//        return karma.Format(
//            err,
//            "unable to run command",
//        )
//    }

//    return nil
//}

func (execution *Execution) SetStdout(writer io.Writer) *Execution {
	execution.stdout = writer
	execution.Execution.SetStdout(writer)

	return execution
}

func (execution *Execution) SetStderr(writer io.Writer) *Execution {
	execution.stderr = writer
	execution.Execution.SetStderr(writer)

	return execution
}
