package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"redisInAction/Chapter03/model"
	"redisInAction/redisConn"
	"redisInAction/utils"

	"testing"
	"time"
)

func TestLoginCookies(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("Test INCR and DECR", func(t *testing.T) {
		client.Conn.Get("key")
		res := client.Conn.Incr("key").Val()
		utils.AssertnumResult(t, 1, res)
		res = client.Conn.IncrBy("key", 15).Val()
		utils.AssertnumResult(t, 16, res)
		res = client.Conn.DecrBy("key", 5).Val()
		utils.AssertnumResult(t, 11, res)
		succ := client.Conn.Set("key", 13, 0).Val()
		utils.AssertStringResult(t, "OK", succ)
		res, _ = client.Conn.Get("key").Int64()
		utils.AssertnumResult(t, 13, res)
		defer client.Reset()
	})

	t.Run("Operation on substring and bit", func(t *testing.T) {
		res := client.Conn.Append("new-string-key", "hello ").Val()
		utils.AssertnumResult(t, 6, res)
		res = client.Conn.Append("new-string-key", "world!").Val()
		utils.AssertnumResult(t, 12, res)
		str := client.Conn.GetRange("new-string-key", 3, 7).Val()
		utils.AssertStringResult(t, "lo wo", str)
		res = client.Conn.SetRange("new-string-key", 0, "H").Val()
		client.Conn.SetRange("new-string-key", 6, "W")
		utils.AssertnumResult(t, 12, 12)
		str = client.Conn.Get("new-string-key").Val()
		utils.AssertStringResult(t, "Hello World!", str)
		res = client.Conn.SetRange("new-string-key", 11, ", how are you?").Val()
		utils.AssertnumResult(t, 25, res)
		str = client.Conn.Get("new-string-key").Val()
		utils.AssertStringResult(t, "Hello World, how are you?", str)
		res = client.Conn.SetBit("another-key", 2, 1).Val()
		utils.AssertnumResult(t, 0, 0)
		res = client.Conn.SetBit("another-key", 7, 1).Val()
		utils.AssertnumResult(t, 0, 0)
		str = client.Conn.Get("another-key").Val()
		utils.AssertStringResult(t, "!", str)
		defer client.Reset()
	})

	t.Run("Operation on list", func(t *testing.T) {
		client.Conn.RPush("list-key", "last")
		client.Conn.LPush("list-key", "first")
		res := client.Conn.RPush("list-key", "new last").Val()
		utils.AssertnumResult(t, 3, res)
		lst := client.Conn.LRange("list-key", 0, -1).Val()
		t.Log("the list is: ", lst)
		str := client.Conn.LPop("list-key").Val()
		utils.AssertStringResult(t, "first", str)
		str = client.Conn.LPop("list-key").Val()
		utils.AssertStringResult(t, "last", str)
		lst = client.Conn.LRange("list-key", 0, -1).Val()
		t.Log("the list is: ", lst)
		res = client.Conn.RPush("list-key", "a", "b", "c").Val()
		utils.AssertnumResult(t, 4, res)
		client.Conn.LTrim("list-key", 2, -1)
		t.Log("the list is: ", fmt.Sprintf("%v", conn.LRange("list-key", 0, -1).Val()))
		defer client.Reset()
	})

	t.Run("Block pop", func(t *testing.T) {
		client.Conn.RPush("list", "item1")
		client.Conn.RPush("list", "item2")
		client.Conn.RPush("list2", "item3")
		item := client.Conn.BRPopLPush("list2", "list", 1*time.Second).Val()
		utils.AssertStringResult(t, "item3", item)
		client.Conn.BRPopLPush("list2", "list", 1*time.Second)
		t.Log("the list is: ", fmt.Sprintf("%v", conn.LRange("list", 0, -1).Val()))
		client.Conn.BRPopLPush("list", "list2", 1*time.Second)
		t.Log("the list is: ", fmt.Sprintf("%v", conn.LRange("list", 0, -1).Val()))
		t.Log("the list2 is: ", fmt.Sprintf("%v", conn.LRange("list2", 0, -1).Val()))
		res := client.Conn.BLPop(1*time.Second, "list", "list2").Val()
		t.Log("the result of blpop: ", res)
		res = client.Conn.BLPop(1*time.Second, "list", "list2").Val()
		t.Log("the result of blpop: ", res)
		res = client.Conn.BLPop(1*time.Second, "list", "list2").Val()
		t.Log("the result of blpop: ", res)
		res = client.Conn.BLPop(1*time.Second, "list", "list2").Val()
		t.Log("the result of blpop: ", res)
		defer client.Reset()
	})

	t.Run("Operation of set", func(t *testing.T) {
		res := client.Conn.SAdd("set-key", "a", "b", "c").Val()
		utils.AssertnumResult(t, 3, res)
		client.Conn.SRem("set-key", "c", "d")
		res = client.Conn.SRem("set-key", "c", "d").Val()
		utils.AssertnumResult(t, 0, res)
		res = client.Conn.SCard("set-key").Val()
		utils.AssertnumResult(t, 2, 2)
		t.Log("all items in set: ", fmt.Sprintf("%v", conn.SMembers("set-key").Val()))
		client.Conn.SMove("set-key", "set-key2", "a")
		client.Conn.SMove("set-key", "set-key2", "c")
		t.Log("all items in set2: ", fmt.Sprintf("%v", conn.SMembers("set-key2").Val()))

		client.Conn.SAdd("skey1", "a", "b", "c", "d")
		client.Conn.SAdd("skey2", "c", "d", "e", "f")
		set := client.Conn.SDiff("skey1", "skey2").Val()
		t.Log("the diff between two set is: ", set)
		set = client.Conn.SInter("skey1", "skey2").Val()
		t.Log("the inter between two set is: ", set)
		set = client.Conn.SUnion("skey1", "skey2").Val()
		t.Log("the union between two set is: ", set)
		defer client.Reset()
	})

	t.Run("Operation on hash", func(t *testing.T) {
		client.Conn.HMSet("hash-key", map[string]interface{}{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		})
		res := client.Conn.HMGet("hash-key", "k2", "k3").Val()
		t.Log("the result of get: ", res)
		length := client.Conn.HLen("hash-key").Val()
		utils.AssertnumResult(t, 3, length)
		client.Conn.HDel("hash-key", "k1", "k2")
		mps := client.Conn.HGetAll("hash-key").Val()
		t.Log("the result of get: ", mps)
		client.Conn.HMSet("hash-key2", map[string]interface{}{
			"short": "hello",
			"long":  "1000",
		})
		strs := client.Conn.HKeys("hash-key2").Val()
		t.Log("the result of hkeys: ", strs)
		isOk := client.Conn.HExists("hash-key2", "num").Val()
		utils.AssertFalse(t, isOk)
		count := client.Conn.HIncrBy("hash-key2", "num", 1).Val()
		utils.AssertnumResult(t, 1, count)
		defer client.Reset()
	})

	t.Run("Operation on zset", func(t *testing.T) {
		res := client.Conn.ZAdd("zset-key", &redis.Z{Member: "a", Score: 3}, &redis.Z{Member: "b", Score: 2},
			&redis.Z{Member: "c", Score: 1}).Val()
		utils.AssertnumResult(t, 3, res)
		res = client.Conn.ZCard("zset-key").Val()
		utils.AssertnumResult(t, 3, res)
		fnum := client.Conn.ZIncrBy("zset-key", 3, "c").Val()
		utils.AssertfloatResult(t, 4.0, fnum)
		fnum = client.Conn.ZScore("zset-key", "b").Val()
		utils.AssertfloatResult(t, 2.0, fnum)
		res = client.Conn.ZRank("zset-key", "c").Val()
		utils.AssertnumResult(t, 2, res)
		res = client.Conn.ZCount("zset-key", "0", "3").Val()
		utils.AssertnumResult(t, 2, res)
		client.Conn.ZRem("zset-key", "b")
		zset := client.Conn.ZRangeWithScores("zset-key", 0, -1).Val()
		t.Log("the result of zrange: ", zset)

		client.Conn.ZAdd("zset-1", &redis.Z{Member: "a", Score: 1}, &redis.Z{Member: "b", Score: 2},
			&redis.Z{Member: "c", Score: 3})
		client.Conn.ZAdd("zset-2", &redis.Z{Member: "b", Score: 4}, &redis.Z{Member: "d", Score: 0},
			&redis.Z{Member: "c", Score: 1})
		client.Conn.ZInterStore("zset-i", &redis.ZStore{Keys:[]string{"zset-1", "zset-2"}})
		zset = client.Conn.ZRangeWithScores("zset-i", 0, -1).Val()
		t.Log("the result of zrange: ", zset)
		client.Conn.ZUnionStore("zset-u", &redis.ZStore{Aggregate: "min", Keys:[]string{"zset-1", "zset-2"}})
		zset = client.Conn.ZRangeWithScores("zset-u", 0, -1).Val()
		t.Log("the result of zrange: ", zset)
		client.Conn.SAdd("set-1", "a", "d")
		client.Conn.ZUnionStore("zset-u2", &redis.ZStore{Keys:[]string{"zset-1", "zset-2"}})
		zset = client.Conn.ZRangeWithScores("zset-u2", 0, -1).Val()
		t.Log("the result of zrange: ", zset)
		defer client.Reset()
	})

	t.Run("Sort operation", func(t *testing.T) {
		client.Conn.RPush("sort-input", 23, 15, 110, 7)
		res := client.Conn.Sort("sort-input", &redis.Sort{Order: "ASC"}).Val()
		t.Log("result of sort: ", res)
		res = client.Conn.Sort("sort-input", &redis.Sort{Alpha: true}).Val()
		t.Log("result of sort: ", res)
		client.Conn.HSet("d-7", "field", 5)
		client.Conn.HSet("d-15", "field", 1)
		client.Conn.HSet("d-23", "field", 9)
		client.Conn.HSet("d-110", "field", 3)
		res = client.Conn.Sort("sort-input", &redis.Sort{By: "d-*->field"}).Val()
		t.Log("result of sort: ", res)
		res = client.Conn.Sort("sort-input", &redis.Sort{By: "d-*->field", Get: []string{"d-*->field"}}).Val()
		t.Log("result of sort: ", res)
		defer client.Reset()
	})

	t.Run("Set expire", func(t *testing.T) {
		client.Conn.Set("key", "value", 0)
		res := client.Conn.Get("key").Val()
		utils.AssertStringResult(t, "value", res)
		client.Conn.Expire("key", 1*time.Second)
		time.Sleep(2 * time.Second)
		res = client.Conn.Get("key").Val()
		utils.AssertStringResult(t, "", res)
		client.Conn.Set("key", "value2", 0)
		client.Conn.Expire("key", 100*time.Second)
		t.Log("the rest time: ", client.Conn.TTL("key").Val())
		defer client.Reset()
	})

	t.Run("Test transaction", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			go client.NotRans()
		}
		time.Sleep(500 * time.Millisecond)

		for i := 0; i < 3; i++ {
			go client.Trans()
		}
		time.Sleep(500 * time.Millisecond)
		defer client.Reset()
	})

	t.Run("Publish and subscribe", func(t *testing.T) {
		go client.RunPubsub()
		client.Publisher(6)
		time.Sleep(1 * time.Second)
		defer client.Reset()
	})
}

