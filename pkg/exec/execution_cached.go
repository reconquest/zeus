package exec

import (
	"bytes"
	"io"
)

// XXX: dead code, not used after refactoring

var (
	cache = map[string]*ExecutionCached{}
)

type ExecutionCached struct {
	*Execution

	stdout bytes.Buffer
	stderr bytes.Buffer
}

func (cached *ExecutionCached) Output() (string, string, error) {
	if cached, ok := cache[cached.String()]; ok {
		return cached.stdout.String(), cached.String(), nil
	}

	stdout, stderr, err := cached.Execution.Output()
	if err != nil {
		return stdout, stderr, err
	}

	cached.stdout.WriteString(stdout)
	cached.stdout.WriteString(stderr)

	cache[cached.String()] = cached

	return stdout, stderr, nil
}

func (cached *ExecutionCached) start(
	fn func(cached *ExecutionCached) error,
) error {
	if _, ok := cache[cached.String()]; ok {
		return nil
	}

	var (
		stdout io.Writer
		stderr io.Writer
	)

	if cached.Execution.stdout != nil {
		stdout = io.MultiWriter(&cached.stdout, stdout)
	}

	if cached.Execution.stderr != nil {
		stderr = io.MultiWriter(&cached.stderr, stderr)
	}

	cached.Execution.SetStdout(stdout)
	cached.Execution.SetStderr(stderr)

	return fn(cached)
}

func (cached *ExecutionCached) Start() error {
	return cached.start(func(cached *ExecutionCached) error {
		err := cached.Execution.Start()
		if err != nil {
			return err
		}

		go func() {
			err := cached.Execution.Wait()
			if err != nil {
				cache[cached.String()] = cached
			}
		}()

		return nil
	})
}

func (cached *ExecutionCached) Run() error {
	return cached.start(func(cached *ExecutionCached) error {
		err := cached.Execution.Run()
		if err != nil {
			return err
		}

		cache[cached.String()] = cached

		return nil
	})
}
