package zfs

import (
	"fmt"
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
		Name       string
		Local      bool
		Inherited  bool
		System     bool
		Snapshot   bool
		Filesystem bool
	}

	PropertyMapping struct {
		Source     string
		Properties []Property
	}
)

func GetPoolProperties(
	requests []PropertyRequest,
	pools ...string,
) ([]PropertyMapping, error) {
	return getProperties(`zpool`, requests, pools...)
}

func GetDatasetProperties(
	requests []PropertyRequest,
	datasets ...string,
) ([]PropertyMapping, error) {
	return getProperties(`zfs`, requests, datasets...)
}

func SetDatasetProperty(dataset string, name string, value string) error {
	err := exec.Exec(
		`zfs`, `set`,
		fmt.Sprintf("%s=%s", name, value),
		dataset,
	).Run()
	if err != nil {
		return karma.
			Describe("name", name).
			Describe("value", value).
			Format(
				err,
				"unabler to run zfs set",
			)
	}

	return nil
}

func getProperties(
	level string,
	requests []PropertyRequest,
	datasets ...string,
) ([]PropertyMapping, error) {
	properties, err := getPropertyList(level, requests, datasets...)
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
	datasets ...string,
) ([]Property, error) {
	type IndexedList struct {
		index map[string]bool
		list  []string
	}

	var (
		names     = []string{}
		inherited = map[string]bool{}

		types, sources IndexedList
	)

	types.index = map[string]bool{}
	sources.index = map[string]bool{}

	set := func(container *IndexedList, flag bool, name string) {
		if flag {
			if !container.index[name] {
				container.list = append(container.list, name)
				container.index[name] = true
			}
		}
	}

	for _, request := range requests {
		names = append(names, request.Name)

		inherited[request.Name] = request.Inherited

		set(&types, request.Snapshot, "snapshot")
		set(&types, request.Filesystem, "filesystem")

		set(&sources, request.Local, "local")
		set(&sources, request.Inherited, "inherited")
		set(&sources, request.System, "none")

		if request.Inherited {
			inherited[request.Name] = true
		}
	}

	args := []string{
		`get`, strings.Join(names, ","),
		`-H`, `-o`, `name,property,value,source`,
		`-p`,
	}

	if len(types.list) > 0 {
		args = append(args, `-t`, strings.Join(types.list, ","))
	}

	if len(sources.list) > 0 {
		args = append(args, `-s`, strings.Join(sources.list, ","))
	}

	if len(datasets) > 0 {
		args = append(args, datasets...)
	}

	switch level {
	case "zpool":
		// ok
	case "zfs":
		// ok
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
			Describe("datasets", datasets).
			Format(
				err,
				"unable to get properties from datasets",
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

		if property.Inherited && !inherited[property.Name] {
			continue
		}

		properties = append(properties, property)
	}

	return properties, nil
}
