package common

var SERVERITY = map[string]string{
	"DEBUG":    "debug",
	"INFO":     "info",
	"WARNING":  "warning",
	"ERROR":    "error",
	"CRITICAL": "critical",
}

var PRECISION = [...]int64{1, 5, 60, 300, 3600, 18000, 86400}

var (
	QUIT        = false
	SAMPLECOUNT = 100
)

var (
	LASTCHECKED        int64 = 0
	ISUNDERMAINTENANCE       = false
)

var (
	CONFIG  = map[string]map[string]interface{}{}
	CHECKED = map[string]int64{}
)
