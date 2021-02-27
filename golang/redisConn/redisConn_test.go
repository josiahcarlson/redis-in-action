package redisConn

import (
	"redisInAction/utils"
	"testing"
)

func TestCheckVersion(t *testing.T) {
	version, err := CheckVersion("6.0.9", "4.0.0");
	utils.AssertTrue(t, err == nil)
	utils.AssertStringResult(t, "6.0.9", version)

	version, err = CheckVersion("3.0", "4.0.0");
	utils.AssertTrue(t, err != nil)
	utils.AssertStringResult(t, "", version)
}
