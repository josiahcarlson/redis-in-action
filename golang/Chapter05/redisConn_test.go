package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"redisInAction/Chapter05/common"
	"redisInAction/Chapter05/model"
	"redisInAction/config"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"strconv"
	"testing"
	"time"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("Test log recent", func(t *testing.T) {
		t.Log("Let's write a few logs to the recent log")
		for i := 0; i < 5; i++ {
			client.LogRecent("test", fmt.Sprintf("this is message %d", i), "info", nil)
		}
		recent := client.Conn.LRange("recent:test:info", 0, -1).Val()
		t.Log("The current recent message log has this many messages:", len(recent))
		t.Log("Those messages include:")
		for _, v := range recent {
			t.Log(v)
		}
		utils.AssertTrue(t, len(recent) >= 5)
		defer client.Conn.FlushDB()
	})

	t.Run("Test log common", func(t *testing.T) {
		t.Log("Let's write some items to the common log")
		for i := 1; i < 6; i++ {
			for j := 0; j < i; j++ {
				client.LogCommon("test", fmt.Sprintf("message-%d", i), "info", 5)
			}
		}
		commons := client.Conn.ZRevRangeWithScores("common:test:info", 0, -1).Val()
		t.Log("The current number of common messages is:", len(commons))
		t.Log("Those common messages are:")
		for _, v := range commons {
			t.Log(v)
		}
		utils.AssertTrue(t, len(commons) >= 5)
		defer client.Conn.FlushDB()
	})

	t.Run("Test counters", func(t *testing.T) {
		t.Log("Let's update some counters for now and a little in the future")
		now := time.Now().Unix()
		for i := 0; i < 10; i++ {
			client.UpdateCounter("test", int64(rand.Intn(5)+1), now+int64(i))
		}
		counter := client.GetCount("test", "1")
		t.Log("We have some per-second counters:", len(counter))
		utils.AssertTrue(t, len(counter) >= 10)
		counter = client.GetCount("test", "5")
		t.Log("We have some per-5-second counters:", len(counter))
		t.Log("These counters include:")
		count := 0
		for _, v := range counter {
			if count == 10 {
				break
			}
			t.Log(v)
			count++
		}
		utils.AssertTrue(t, len(counter) >= 2)

		t.Log("Let's clean out some counters by setting our sample count to 0")
		common.SAMPLECOUNT = 0
		go client.CleanCounters()
		time.Sleep(1 * time.Second)
		common.QUIT = true
		counter = client.GetCount("test", "86400")
		t.Log("Did we clean out all of the counters?", len(counter))
		utils.AssertTrue(t, len(counter) == 0)
		defer client.Conn.FlushDB()
	})

	t.Run("Test stats", func(t *testing.T) {
		t.Log("Let's add some data for our statistics!")
		var res []redis.Cmder
		for i := 0; i < 5; i++ {
			res = client.UpdateStats("temp", "example", utils.RandomFloat(5, 15), 5)
		}
		t.Log("We have some aggregate statistics:", res)
		rr := client.GetStats("temp", "example")
		t.Log("Which we can also fetch manually:")
		for k, v := range rr {
			t.Log(k, v)
		}
		utils.AssertTrue(t, rr["count"] >= 5)
		defer client.Conn.FlushDB()
	})

	t.Run("Test access time", func(t *testing.T) {
		t.Log("Let's calculate some access times...")
		for i := 0; i < 10; i++ {
			client.AccessTime(fmt.Sprintf("req-%s", strconv.Itoa(i)), func() {
				time.Sleep(time.Duration(rand.Int63n(5) * 500) * time.Millisecond)
			})
		}
		t.Log("The slowest access times are:")
		atimes := client.Conn.ZRevRangeWithScores("slowest:AccessTime", 0, -1).Val()
		for _, v := range atimes[:10] {
			t.Log(v.Member)
		}
		utils.AssertTrue(t, len(atimes) >= 10)
		defer client.Conn.FlushDB()
	})

	t.Run("Test is under maintenance", func(t *testing.T) {
		t.Log("Are we under maintenance (we shouldn't be)?", client.IsUnderMaintenance())
		client.Conn.Set("is-under-maintenance", "yes", 0)
		t.Log("We cached this, so it should be the same:", client.IsUnderMaintenance())
		time.Sleep(2 * time.Second)
		t.Log("But after a sleep, it should change:", client.IsUnderMaintenance())
		t.Log("Cleaning up...")
		client.Conn.Del("is-under-maintenance")
		time.Sleep(2 * time.Second)
		t.Log("Should be False again:", client.IsUnderMaintenance())
		defer client.Conn.FlushDB()
	})

	t.Run("Test ip lookup", func(t *testing.T) {
		t.Log("Importing IP addresses to Redis... (this may take a while)")
		client.ImportIpsToRedis(config.FilePath + "GeoLite2-City-Blocks-IPv4.csv")
		ranges := client.Conn.ZCard("ip2cityid:").Val()
		t.Log("Loaded ranges into Redis:", ranges)
		utils.AssertTrue(t, ranges > 1000)

		t.Log("Importing Location lookups to Redis... (this may take a while)")
		client.ImportCityToRedis(config.FilePath + "GeoLite2-City-Locations-en.csv")
		cities := client.Conn.HLen("cityid2city:").Val()
		t.Log("Loaded city lookups into Redis:", cities)
		utils.AssertTrue(t, cities > 1000)

		t.Log("Let's lookup some locations!")
		for i := 0; i < 5; i++ {
			ip := fmt.Sprintf("%s.%s.%s.%s",
				strconv.Itoa(rand.Intn(254)+1), utils.RandomString(256), utils.RandomString(256),
				utils.RandomString(256))
			t.Log(ip, client.FindCityByIp(ip))
		}
		defer client.Conn.FlushDB()
	})
}
