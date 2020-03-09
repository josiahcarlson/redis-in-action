package main

import (
	"github.com/satori/go.uuid"
	"redisInAction/Chapter02/common"
	"redisInAction/Chapter02/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"testing"
	"time"
)

func TestLoginCookies(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	token := uuid.NewV4().String()

	t.Run("Test UpdateToken", func(t *testing.T) {
		client.UpdateToken(token, "username", "itemX")
		t.Log("We just logged-in/update token: \n", token)
		t.Log("For user: ", "username\n")
		t.Log("\nWhat username do we get when we look-up that token?\n")
		r := client.CheckToken(token)
		t.Log("username: ", r)
		utils.AssertStringResult(t, "username", r)

		t.Log("Let's drop the maximum number of cookies to 0 to clean them out\n")
		t.Log("We will start a thread to do the cleaning, while we stop it later\n")

		common.LIMIT = 0
		go client.CleanSessions()
		time.Sleep(1 * time.Second)
		common.QUIT = true
		time.Sleep(2 * time.Second)

		utils.AssertThread(t, common.FLAG)

		s := conn.HLen("login:").Val()
		t.Log("The current number of sessions still available is:", s)
		utils.AssertnumResult(t, 1, s)
		defer client.Reset()
	})

	t.Run("Test shopping cart cookie", func(t *testing.T) {
		t.Log("We'll refresh our session...")
		client.UpdateToken(token, "username", "itemX")
		t.Log("And add an item to the shopping cart")
		client.AddToCart(token, "itemY", 3)
		r := conn.HGetAll("cart:" + token).Val()
		t.Log("Our shopping cart currently has:", r)

		utils.AssertTrue(t, len(r) >= 1)

		t.Log("Let's clean out our sessions and carts")
		common.LIMIT = 0
		go client.CleanFullSession()
		time.Sleep(1 * time.Second)
		common.QUIT = true
		time.Sleep(2 * time.Second)
		utils.AssertThread(t, common.FLAG)

		r = conn.HGetAll("cart:" + token).Val()
		t.Log("Our shopping cart now contains:", r)
		defer client.Reset()
	})

	t.Run("Test cache request", func(t *testing.T) {
		client.UpdateToken(token, "username", "itemX")
		url := "http://test.com/?item=itemX"
		t.Log("We are going to cache a simple request against", url)
		result := client.CacheRequest(url, func(request string) string {
			return "content for " + request
		})
		t.Log("We got initial content: ", result)
		utils.AssertTrue(t, result != "")

		t.Log("To test that we've cached the request, we'll pass a bad callback")
		result2 := client.CacheRequest(url, nil)
		t.Log("We ended up getting the same response!", result2)
		utils.AssertStringResult(t, result, result2)
		utils.AssertFalse(t, client.CanCache("http://test.com/"))
		utils.AssertFalse(t, client.CanCache("http://test.com/?item=itemX&_=1234536"))
		client.Conn.FlushDB()
	})

	t.Run("Test cache row", func(t *testing.T) {
		t.Log("First, let's schedule caching of itemX every 5 seconds")
		client.ScheduleRowCache("itemX", 5)
		t.Log("Our schedule looks like:")
		s := client.Conn.ZRangeWithScores("schedule:", 0, -1).Val()
		t.Log(s[0].Member, s[0].Score)
		utils.AssertTrue(t, len(s) != 0)

		t.Log("We'll start a caching thread that will cache the data...")
		go client.CacheRows()
		time.Sleep(1 * time.Second)
		t.Log("Our cached data looks like:")
		r := client.Conn.Get("inv:itemX").Val()
		t.Log(r)
		utils.AssertTrue(t, len(r) != 0)

		t.Log("We'll check again in 5 seconds...")
		time.Sleep(5 * time.Second)
		t.Log("Notice that the data has changed...")
		r2 := client.Conn.Get("inv:itemX").Val()
		t.Log(r2)
		utils.AssertTrue(t, len(r) != 0)
		utils.AssertTrue(t, r != r2)

		t.Log("Let's force un-caching")
		client.ScheduleRowCache("itemX", -1)
		time.Sleep(1 * time.Second)
		r = client.Conn.Get("inv:itemX").Val()
		t.Log("The cache was cleared?", r == "")
		utils.AssertFalse(t, r != "")

		common.QUIT = true
		time.Sleep(2 * time.Second)
		utils.AssertThread(t, common.FLAG)
		defer client.Conn.FlushDB()
	})
}



