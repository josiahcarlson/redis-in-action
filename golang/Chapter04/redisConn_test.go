package main

import (
	"redisInAction/Chapter04/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"testing"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("Test list item", func(t *testing.T) {
		t.Log("We need to set up just enough state so that a user can list an item")
		seller := "userX"
		item := "itemX"
		client.Conn.SAdd("inventory:"+seller, item)
		i := client.Conn.SMembers("inventory:" + seller).Val()
		t.Log("The user's inventory has:", i)
		utils.AssertTrue(t, len(i) > 0)

		t.Log("Listing the item...")
		l := client.ListItem(item, seller, 10)
		t.Log("Listing the item succeeded?", l)
		utils.AssertTrue(t, l)
		r := client.Conn.ZRangeWithScores("market:", 0, -1).Val()
		t.Log("The market contains:", r)
		t.Log("The inventory: ", client.Conn.Get("inventory:"+seller).Val())
	})

	t.Run("Test purchase item", func(t *testing.T) {
		client.ListItem("itemX", "userX", 10)
		t.Log("We need to set up just enough state so a user can buy an item")
		buyer := "userY"
		client.Conn.HSet("users:userY", "funds", 125)
		r := client.Conn.HGetAll("users:userY").Val()
		t.Log("The user has some money:", r)
		utils.AssertTrue(t, len(r) != 0)

		p := client.PurchaseItem("userY", "itemX", "userX", 10)
		t.Log("Purchasing an item succeeded?", p)
		utils.AssertTrue(t, p)
		r = client.Conn.HGetAll("users:userY").Val()
		t.Log("Their money is now:", r)
		utils.AssertTrue(t, len(r) > 0)
		i := client.Conn.SMembers("inventory:" + buyer).Val()
		t.Log("Their inventory is now:", i)
		utils.AssertTrue(t, len(i) > 0)
		utils.AssertTrue(t, client.Conn.ZScore("market:", "itemX.userX").Val() == 0)
	})
	defer client.Conn.FlushDB()
}

func BenchmarkUpdateToken(b *testing.B) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	b.Run("BenchmarkUpdateTokenPipeline", func(b *testing.B) {
		count := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count++
			client.UpdateTokenPipeline("token", "user", "item")
		}
		defer client.Conn.FlushDB()
	})

	b.Run("BenchmarkUpdateToken", func(b *testing.B) {
		count := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			count++
			client.UpdateToken("token", "user", "item")
		}
		defer client.Conn.FlushDB()
	})
}
