package errs

import (
	"fmt"
	"strings"

	"github.com/reconquest/karma-go"
	"github.com/reconquest/zeus/pkg/zfs"
)

func UnsupportedPropertyValue(
	property zfs.Property,
	supported []string,
) error {
	return karma.
		Describe("dataset", property.Source).
		Describe("property", property.Name).
		Describe("value", property.Value).
		Reason(
			fmt.Errorf(
				strings.Join(
					[]string{
						"unsuported value given for property",
						"supported values are: %q",
					},
					"\n",
				),
				supported,
			),
		)
}
