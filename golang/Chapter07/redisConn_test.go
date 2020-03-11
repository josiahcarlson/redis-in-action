package main

import (
	"github.com/go-redis/redis/v7"
	"redisInAction/Chapter07/common"
	"redisInAction/Chapter07/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewClient(conn)

	t.Run("Test index document", func(t *testing.T) {
		t.Log("We're tokenizing some content...")
		tokens := model.Tokenize(common.CONTENT)
		t.Log("Those tokens are:", tokens)
		utils.AssertTrue(t, len(tokens) > 0)

		t.Log("And now we are indexing that content...")
		r := client.IndexDocument("test", common.CONTENT)
		utils.AssertnumResult(t, int64(len(tokens)), r)
		for _, v := range tokens {
			utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+v).Val(), []string{"test"}))
		}
		defer client.Conn.FlushDB()
	})

	t.Run("Test set operations", func(t *testing.T) {
		client.IndexDocument("test", common.CONTENT)

		r := client.Intersect([]string{"content", "indexed"}, 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{"test"}))

		r = client.Intersect([]string{"content", "ignored"}, 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{}))

		r = client.Union([]string{"content", "ignored"}, 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{"test"}))

		r = client.Difference([]string{"content", "ignored"}, 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{"test"}))

		r = client.Difference([]string{"content", "indexed"}, 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{}))
		defer client.Conn.FlushDB()
	})

	t.Run("Test parse query", func(t *testing.T) {
		query := "test query without stopwords"
		all, unwanted := model.Parse(query)
		utils.AssertTrue(t, reflect.DeepEqual(all, [][]string{{"test"}, {"query"}, {"without"}, {"stopwords"}}))
		utils.AssertTrue(t, len(unwanted) == 0)

		all, unwanted = [][]string{}, []string{}
		query = "test +query without -stopwords"
		all, unwanted = model.Parse(query)
		utils.AssertTrue(t, reflect.DeepEqual(all, [][]string{{"test", "query"}, {"without"}}))
		utils.AssertTrue(t, reflect.DeepEqual(unwanted, []string{"stopwords"}))
	})

	t.Run("Test parse and search", func(t *testing.T) {
		t.Log("And now we are testing search...")
		client.IndexDocument("test", common.CONTENT)

		r := client.ParseAndSearch("content", 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{"test"}))

		r = client.ParseAndSearch("content indexed random", 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{"test"}))

		r = client.ParseAndSearch("content +indexed random", 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{"test"}))

		r = client.ParseAndSearch("content indexed +random", 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{"test"}))

		r = client.ParseAndSearch("content indexed -random", 30)
		utils.AssertTrue(t, reflect.DeepEqual(client.Conn.SMembers("idx:"+r).Val(), []string{}))

		t.Log("Which passed!")
		defer client.Conn.FlushDB()
	})

	t.Run("Test search with sort", func(t *testing.T) {
		t.Log("And now let's test searching with sorting...")

		client.IndexDocument("test", common.CONTENT)
		client.IndexDocument("test2", common.CONTENT)
		client.Conn.HMSet("kb:doc:test", "updated", 12345, "id", 10)
		client.Conn.HMSet("kb:doc:test2", "updated", 54321, "id", 1)

		r, _ := client.SearchAndSort("content", "", 300, "-updated", 0, 20)
		utils.AssertTrue(t, reflect.DeepEqual(r, []string{"test2", "test"}))

		r, _ = client.SearchAndSort("content", "", 300, "-id", 0, 20)
		utils.AssertTrue(t, reflect.DeepEqual(r, []string{"test", "test2"}))
		t.Log("Which passed!")
		client.Conn.FlushDB()
	})

	t.Run("Test search with zsort", func(t *testing.T) {
		t.Log("And now let's test searching with sorting via zset...")

		client.IndexDocument("test", common.CONTENT)
		client.IndexDocument("test2", common.CONTENT)
		client.Conn.ZAdd("idx:sort:update", &redis.Z{Member: "test", Score: 12345},
			&redis.Z{Member: "test2", Score: 54321})
		client.Conn.ZAdd("idx:sort:votes", &redis.Z{Member: "test", Score: 10},
			&redis.Z{Member: "test2", Score: 1})

		r, _ := client.SearchAndZsort("content", "", 300, 1, 0,
			0, 20, false)
		utils.AssertTrue(t, reflect.DeepEqual(r, []string{"test", "test2"}))

		r, _ = client.SearchAndZsort("content", "", 300, 0, 1,
			0, 20, false)
		utils.AssertTrue(t, reflect.DeepEqual(r, []string{"test2", "test"}))
		t.Log("Which passed!")
		client.Conn.FlushDB()
	})

	t.Run("Test string to score", func(t *testing.T) {
		words := strings.Fields("these are some words that will be sorted")
		pairs := make([]map[string]int, 0, len(words))
		pairs2 := make([]map[string]int, 0, len(words))
		for _, word := range words {
			pairs = append(pairs, map[string]int{word: client.StringToScore(word, false)})
			pairs2 = append(pairs2, map[string]int{word: client.StringToScore(word, false)})
		}

		sort.Slice(pairs, func(i, j int) bool {
			var k1, k2 string
			for k1 = range pairs[i] {
			}
			for k2 = range pairs[j] {
			}
			return k1 < k2
		})
		sort.Slice(pairs2, func(i, j int) bool {
			var v1, v2 int
			for _, v1 = range pairs2[i] {
			}
			for _, v2 = range pairs2[j] {
			}
			return v1 < v2
		})
		utils.AssertTrue(t, reflect.DeepEqual(pairs, pairs2))
	})

	t.Run("Test index and Target ads", func(t *testing.T) {
		client.IndexAd("1", []string{"USA", "CA"}, common.CONTENT, "cpc", 0.25)
		client.IndexAd("2", []string{"USA", "VA"}, common.CONTENT+"wooooo", "cpc", 0.125)

		var adId, targetId string
		for i := 0; i < 100; i++ {
			targetId, adId = client.TargetAds([]string{"USA"}, common.CONTENT)
		}
		utils.AssertTrue(t, adId == "1")

		_, r := client.TargetAds([]string{"VA"}, "wooooo")
		utils.AssertTrue(t, r == "2")

		res := map[string]float64{}
		for _, v := range client.Conn.ZRangeWithScores("idx:ad:value:", 0, -1).Val() {
			res[v.Member.(string)] = v.Score
		}
		utils.AssertTrue(t, reflect.DeepEqual(res, map[string]float64{"2": 0.125, "1": 0.25}))
		for _, v := range client.Conn.ZRangeWithScores("ad:baseValue:", 0, -1).Val() {
			res[v.Member.(string)] = v.Score
		}
		utils.AssertTrue(t, reflect.DeepEqual(res, map[string]float64{"2": 0.125, "1": 0.25}))

		client.RecordClick(targetId, adId, false)
		res = map[string]float64{}
		for _, v := range client.Conn.ZRangeWithScores("idx:ad:value:", 0, -1).Val() {
			res[v.Member.(string)] = v.Score
		}

		utils.AssertTrue(t, reflect.DeepEqual(res, map[string]float64{"2": 0.125, "1": 2.5}))
		for _, v := range client.Conn.ZRangeWithScores("ad:baseValue:", 0, -1).Val() {
			res[v.Member.(string)] = v.Score
		}

		utils.AssertTrue(t, reflect.DeepEqual(res, map[string]float64{"2": 0.125, "1": 0.25}))
		defer client.Conn.FlushDB()
	})

	t.Run("Test is qualified for job", func(t *testing.T) {
		client.AddJob("test", []string{"q1", "q2", "q3"})
		res := client.IsQualified("test", []string{"q1", "q2", "q3"})
		utils.AssertTrue(t, len(res) == 0)
		res = client.IsQualified("test", []string{"q1", "q2"})
		utils.AssertFalse(t, len(res) == 0)
		client.Conn.FlushDB()
	})

	t.Run("Test index and find jobs", func(t *testing.T) {
		client.IndexJob("test1", []string{"q1", "q2", "q3"})
		client.IndexJob("test2", []string{"q1", "q3", "q4"})
		client.IndexJob("test3", []string{"q1", "q3", "q5"})

		utils.AssertTrue(t, reflect.DeepEqual(client.FindJobs([]string{"q1"}), []string{}))
		utils.AssertTrue(t, reflect.DeepEqual(client.FindJobs([]string{"q1", "q3", "q4"}), []string{"test2"}))
		utils.AssertTrue(t, reflect.DeepEqual(client.FindJobs([]string{"q1", "q3", "q5"}), []string{"test3"}))
		utils.AssertTrue(t, reflect.DeepEqual(client.FindJobs([]string{"q1", "q2", "q3", "q4", "q5"}),
			[]string{"test1", "test2", "test3"}))
		client.Conn.FlushDB()
	})

	t.Run("Test index and find jobs levels", func(t *testing.T) {
		t.Log("now testing find jobs with levels ...")
		client.IndexJobLevels("job1", map[string]int64{"q1": 1})
		client.IndexJobLevels("job2", map[string]int64{"q1": 0, "q2": 2})

		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q1": 0}), []string{}))
		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q1": 1}), []string{"job1"}))
		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q1": 2}), []string{"job1"}))
		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q2": 1}), []string{}))
		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q2": 2}), []string{}))
		utils.AssertTrue(t,
			reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q1": 0, "q2": 1}), []string{}))
		utils.AssertTrue(t,
			reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q1": 0, "q2": 2}), []string{"job2"}))
		utils.AssertTrue(t,
			reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q1": 1, "q2": 1}), []string{"job1"}))
		utils.AssertTrue(t,
			reflect.DeepEqual(client.SearchJobLevels(map[string]int64{"q1": 1, "q2": 2}), []string{"job1", "job2"}))
		t.Log("which passed")
		client.Conn.FlushDB()
	})

	t.Run("Test index and find jobs years", func(t *testing.T) {
		t.Log("now testing find jobs with years ...")
		client.IndexJobYears("job1", map[string]int64{"q1": 1})
		client.IndexJobYears("job2", map[string]int64{"q1": 0, "q2": 2})

		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q1": 0}), []string{}))
		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q1":1}), []string{"job1"}))
		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q1":2}), []string{"job1"}))
		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q2":1}), []string{}))
		utils.AssertTrue(t, reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q2":2}), []string{}))
		utils.AssertTrue(t,
			reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q1":0, "q2":1}), []string{}))
		utils.AssertTrue(t,
			reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q1":0, "q2":2}), []string{"job2"}))
		utils.AssertTrue(t,
			reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q1":1, "q2":1}), []string{"job1"}))
		utils.AssertTrue(t,
			reflect.DeepEqual(client.SearchJobYears(map[string]int64{"q1":1, "q2":2}), []string{"job1", "job2"}))
		t.Log("which passed")
		client.Conn.FlushDB()
	})
}
