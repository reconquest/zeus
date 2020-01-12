package zfs

import (
	"strings"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/exec"
)

type (
	Property struct {
		PropertyRequest
		Value  string
		Source string
	}

	PropertyRequest struct {
		Name      string
		Inherited bool
	}

	PropertyMapping struct {
		Source     string
		Properties []Property
	}
)

func GetPoolProperties(
	requests []PropertyRequest,
) ([]PropertyMapping, error) {
	return getProperties(`zpool`, requests)
}

func GetDatasetProperties(
	requests []PropertyRequest,
) ([]PropertyMapping, error) {
	return getProperties(`zfs`, requests)
}

func getProperties(
	level string,
	requests []PropertyRequest,
) ([]PropertyMapping, error) {
	properties, err := getPropertyList(level, requests)
	if err != nil {
		return nil, err
	}

	var (
		mappings = []PropertyMapping{}
		index    = map[string][]Property{}
	)

	for _, property := range properties {
		if index[property.Source] == nil {
			mappings = append(mappings, PropertyMapping{
				Source: property.Source,
			})
		}

		index[property.Source] = append(index[property.Source], property)
	}

	for i, _ := range mappings {
		mappings[i].Properties = index[mappings[i].Source]
	}

	return mappings, nil
}

func getPropertyList(
	level string,
	requests []PropertyRequest,
) ([]Property, error) {
	var (
		names          = []string{}
		allowInherited = map[string]bool{}
	)

	for _, request := range requests {
		names = append(names, request.Name)
		allowInherited[request.Name] = request.Inherited
	}

	args := []string{
		`get`, strings.Join(names, ","),
		`-H`, `-o`, `name,property,value,source`,
		`-p`, `-s`, `local,inherited`,
		`-t`, `filesystem`,
	}

	switch level {
	case "zpool":
		// ok
	case "zfs":
		args = append(args, `-t`, `filesystem`)
	default:
		return nil, karma.
			Describe("level", level).
			Reason(
				"unexpected `level` value, supported values are: 'zpool', 'zfs'",
			)
	}

	command := exec.Exec(level, args...)

	stdout, _, err := command.Output()
	if err != nil {
		return nil, karma.
			Describe("requests", requests).
			Format(
				err,
				"unable to get properties from `%s`",
				level,
			)
	}

	var properties []Property

	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if line == "" {
			break
		}

		fields := strings.SplitN(line, "\t", 4)
		if len(fields) != 4 {
			return nil, karma.
				Describe("command", command).
				Describe("line", line).
				Format(
					err,
					"unexpected number of fields in property get response",
				)
		}

		// skip unset values
		if fields[2] == "-" {
			continue
		}

		property := Property{
			Source: fields[0],
			Value:  fields[2],
		}

		property.Name = fields[1]

		if strings.HasPrefix(fields[3], "inherited from ") {
			property.Inherited = true
		}

		if property.Inherited && !allowInherited[property.Name] {
			continue
		}

		properties = append(properties, property)
	}

	return properties, nil
}
