package log

import (
	"fmt"
	"regexp"

	"github.com/kovetskiy/lorg"
	"github.com/reconquest/colorgful"
)

var (
	theme = colorgful.MustApplyDefaultTheme(
		`${time:2006-01-02 15:04:05.000} ${level:%s:left:true} ${prefix}%s`,
		colorgful.Default,
	)
)

func init() {
	lorg.SetFormat(theme)
	lorg.SetOutput(theme)

	lorg.SetIndentLines(true)
	lorg.SetShiftIndent(getShiftIndent(""))
}

var (
	Fatal   = lorg.Fatal
	Error   = lorg.Error
	Warning = lorg.Warning
	Info    = lorg.Info
	Debug   = lorg.Debug
	Trace   = lorg.Trace

	Fatalf   = lorg.Fatalf
	Errorf   = lorg.Errorf
	Warningf = lorg.Warningf
	Infof    = lorg.Infof
	Debugf   = lorg.Debugf
	Tracef   = lorg.Tracef

	SetLevel = lorg.SetLevel
)

func NewChildWithPrefix(prefix string) *lorg.Log {
	log := lorg.NewChildWithPrefix(prefix)
	log.SetShiftIndent(getShiftIndent(prefix))
	return log
}

func getShiftIndent(prefix string) int {
	return len(
		regexp.MustCompile(`\x1b\[[^m]+m`).ReplaceAllString(
			fmt.Sprintf(theme.Render(lorg.LevelWarning, prefix), ""), "",
		),
	)
}
