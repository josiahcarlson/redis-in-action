package main

import (
	"fmt"
	"redisInAction/Chapter08/common"
	"redisInAction/Chapter08/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"strconv"
	"testing"
	"time"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("Test create user and status", func(t *testing.T) {
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User") == "1")
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User2") == "")

		utils.AssertTrue(t, client.CreateStatus("1", "This is a new status message",
			map[string]interface{}{}) == "1")
		utils.AssertTrue(t, client.Conn.HGet("user:1", "posts").Val() == "1")
		defer client.Conn.FlushDB()
	})

	t.Run("Test follow and unfollow user", func(t *testing.T) {
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User") == "1")
		utils.AssertTrue(t, client.CreateUser("TestUser2", "Test User2") == "2")

		utils.AssertTrue(t, client.FollowUser("1", "2"))
		utils.AssertTrue(t, client.Conn.ZCard("followers:2").Val() == 1)
		utils.AssertTrue(t, client.Conn.ZCard("followers:1").Val() == 0)
		utils.AssertTrue(t, client.Conn.ZCard("following:1").Val() == 1)
		utils.AssertTrue(t, client.Conn.ZCard("following:2").Val() == 0)
		utils.AssertTrue(t, client.Conn.HGet("user:1", "following").Val() == "1")
		utils.AssertTrue(t, client.Conn.HGet("user:2", "following").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:1", "followers").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:2", "followers").Val() == "1")

		utils.AssertFalse(t, client.UnfollowUser("2", "1"))
		utils.AssertTrue(t, client.UnfollowUser("1", "2"))
		utils.AssertTrue(t, client.Conn.ZCard("followers:2").Val() == 0)
		utils.AssertTrue(t, client.Conn.ZCard("followers:1").Val() == 0)
		utils.AssertTrue(t, client.Conn.ZCard("following:1").Val() == 0)
		utils.AssertTrue(t, client.Conn.ZCard("following:2").Val() == 0)
		utils.AssertTrue(t, client.Conn.HGet("user:1", "following").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:2", "following").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:1", "followers").Val() == "0")
		utils.AssertTrue(t, client.Conn.HGet("user:2", "followers").Val() == "0")
		defer client.Conn.FlushDB()
	})

	t.Run("Test syndicate status", func(t *testing.T) {
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User") == "1")
		utils.AssertTrue(t, client.CreateUser("TestUser2", "Test User2") == "2")
		utils.AssertTrue(t, client.FollowUser("1", "2"))
		utils.AssertTrue(t, client.Conn.ZCard("followers:2").Val() == 1)
		utils.AssertTrue(t, client.Conn.HGet("user:1", "following").Val() == "1")
		utils.AssertTrue(t, client.PostStatus("2", "this is some messages content",
			map[string]interface{}{}) == "1")
		utils.AssertnumResult(t, 1, int64(len(client.GetStatusMessage("1",
			"home", 1, 30))))

		for i := 3; i < 11; i++ {
			utils.AssertTrue(t, client.CreateUser(fmt.Sprintf("TestUser%s", strconv.Itoa(i)),
				fmt.Sprintf("Test User%s", strconv.Itoa(i))) == strconv.Itoa(i))
			client.FollowUser(strconv.Itoa(i), "2")
		}
		common.Postperpass = 5

		utils.AssertTrue(t, client.PostStatus("2",
			"this is some other message content", map[string]interface{}{}) == "2")
		time.Sleep(100 * time.Millisecond)
		utils.AssertnumResult(t, 2, int64(len(client.GetStatusMessage("9",
			"home", 1, 30))))
		utils.AssertTrue(t, client.UnfollowUser("1", "2"))
		utils.AssertnumResult(t, 0, int64(len(client.GetStatusMessage("1",
			"home", 1, 30))))
		client.Conn.FlushDB()
	})

	t.Run("Test refill timeline", func(t *testing.T) {
		utils.AssertTrue(t, client.CreateUser("TestUser", "Test User") == "1")
		utils.AssertTrue(t, client.CreateUser("TestUser2", "Test User2") == "2")
		utils.AssertTrue(t, client.CreateUser("TestUser3", "Test User3") == "3")

		utils.AssertTrue(t, client.FollowUser("1", "2"))
		utils.AssertTrue(t, client.FollowUser("1", "3"))
		common.Hometimelinesize = 5

		for i := 0; i < 10; i++ {
			utils.AssertTrue(t, client.PostStatus("2", "message", map[string]interface{}{}) != "")
			utils.AssertTrue(t, client.PostStatus("3", "message", map[string]interface{}{}) != "")
			time.Sleep(50 * time.Millisecond)
		}

		utils.AssertnumResult(t, 5, int64(len(client.GetStatusMessage("1",
			"home", 1, 30))))
		utils.AssertTrue(t, client.UnfollowUser("1", "2"))
		utils.AssertTrue(t, len(client.GetStatusMessage("1",
			"home", 1, 30)) < 5)

		client.RefillTimeline("following:1", "home:1", 0)
		messages := client.GetStatusMessage("1", "home", 1, 30)
		utils.AssertnumResult(t, 5, int64(len(messages)))
		for _, msg := range messages {
			utils.AssertTrue(t, msg["uid"] == "3")
		}

		client.DeleteStatus("3", messages[len(messages)-1]["id"])
		utils.AssertnumResult(t, 4,
			int64(len(client.GetStatusMessage("1", "home", 1, 30))))
		utils.AssertnumResult(t, 5, client.Conn.ZCard("home:1").Val())
		client.CleanTimeLines("3", messages[len(messages)-1]["id"], 0, false)
		utils.AssertnumResult(t, 4, client.Conn.ZCard("home:1").Val())
		defer client.Conn.FlushDB()
	})
}
