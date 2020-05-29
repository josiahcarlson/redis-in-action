package model

import (
	"crypto"
	"encoding/hex"
	"encoding/json"
	"github.com/go-redis/redis/v7"
	"log"
	"net/url"
	"redisInAction/Chapter02/common"
	"redisInAction/Chapter02/repository"
	"redisInAction/utils"
	"strings"
	"sync/atomic"
	"time"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func (r *Client) CheckToken(token string) string {
	return r.Conn.HGet("login:", token).Val()
}

func (r *Client) UpdateToken(token, user, item string) {
	timestamp := time.Now().Unix()
	r.Conn.HSet("login:", token, user)
	r.Conn.ZAdd("recent", &redis.Z{Score: float64(timestamp), Member: token})
	if item != "" {
		r.Conn.ZAdd("viewed:"+token, item, timestamp)
		r.Conn.ZRemRangeByRank("viewed:"+token, 0, -26)
	}
}

func (r *Client) CleanSessions() {
	for !common.QUIT {
		size := r.Conn.ZCard("recent:").Val()
		if size <= common.LIMIT {
			time.Sleep(1 * time.Second)
			continue
		}

		endIndex := utils.Min(size-common.LIMIT, 100)
		tokens := r.Conn.ZRange("recent:", 0, endIndex-1).Val()

		var sessionKey []string
		for _, token := range tokens {
			sessionKey = append(sessionKey, token)
		}

		r.Conn.Del(sessionKey...)
		r.Conn.HDel("login:", tokens...)
		r.Conn.ZRem("recent:", tokens)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *Client) AddToCart(session, item string, count int) {
	switch {
	case count <= 0:
		r.Conn.HDel("cart:"+session, item)
	default:
		r.Conn.HSet("cart:"+session, item, count)
	}
}

func (r *Client) CleanFullSession() {
	for !common.QUIT {
		size := r.Conn.ZCard("recent:").Val()
		if size <= common.LIMIT {
			time.Sleep(1 * time.Second)
			continue
		}

		endIndex := utils.Min(size-common.LIMIT, 100)
		sessions := r.Conn.ZRange("recent:", 0, endIndex-1).Val()

		var sessionKeys []string
		for _, sess := range sessions {
			sessionKeys = append(sessionKeys, "viewed:"+sess)
			sessionKeys = append(sessionKeys, "cart:"+sess)
		}

		r.Conn.Del(sessionKeys...)
		r.Conn.HDel("login:", sessions...)
		r.Conn.ZRem("recent:", sessions)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *Client) CacheRequest(request string, callback func(string) string) string {
	if r.CanCache(request) {
		return callback(request)
	}

	pageKey := "cache:" + hashRequest(request)
	content := r.Conn.Get(pageKey).Val()

	if content == "" {
		content = callback(request)
		r.Conn.Set(pageKey, content, 300*time.Second)
	}
	return content
}

func (r *Client) ScheduleRowCache(rowId string, delay int64) {
	r.Conn.ZAdd("delay:", &redis.Z{Member: rowId, Score: float64(delay)})
	r.Conn.ZAdd("schedule:", &redis.Z{Member: rowId, Score: float64(time.Now().Unix())})
}

func (r *Client) CacheRows() {
	for !common.QUIT {
		next := r.Conn.ZRangeWithScores("schedule:", 0, 0).Val()
		now := time.Now().Unix()
		if len(next) == 0 || next[0].Score > float64(now) {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		rowId := next[0].Member.(string)
		delay := r.Conn.ZScore("delay:", rowId).Val()
		if delay <= 0 {
			r.Conn.ZRem("delay:", rowId)
			r.Conn.ZRem("schedule:", rowId)
			r.Conn.Del("inv:" + rowId)
			continue
		}

		row := repository.Get(rowId)
		r.Conn.ZAdd("schedule:", &redis.Z{Member: rowId, Score: float64(now) + delay})
		jsonRow, err := json.Marshal(row)
		if err != nil {
			log.Fatalf("marshal json failed, data is: %v, err is: %v\n", row, err)
		}
		r.Conn.Set("inv:"+rowId, jsonRow, 0)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *Client) UpdateTokenModified(token, user string, item string) {
	timestamp := time.Now().Unix()
	r.Conn.HSet("login:", token, user)
	r.Conn.ZAdd("recent:", &redis.Z{Score: float64(timestamp), Member: token})
	if item != "" {
		r.Conn.ZAdd("viewed:"+token, item, timestamp)
		r.Conn.ZRemRangeByRank("viewed:"+token, 0, -26)
		r.Conn.ZIncrBy("viewed:", -1, item)
	}
}

func (r *Client) RescaleViewed() {
	for !common.QUIT {
		r.Conn.ZRemRangeByRank("viewed:", 20000, -1)
		r.Conn.ZInterStore("viewed:", &redis.ZStore{Weights: []float64{0.5}, Keys: []string{"viewed:"}})
		time.Sleep(300 * time.Second)
	}
}

func (r *Client) CanCache(request string) bool {
	itemId := extractItemId(request)
	if itemId == "" || isDynamic(request) {
		return false
	}
	rank := r.Conn.ZRank("viewed:", itemId).Val()
	return rank != 0 && rank < 10000
}

func (r *Client) Reset() {
	r.Conn.FlushDB()

	common.QUIT = false
	common.LIMIT = 10000000
	common.FLAG = 1
}

func extractItemId(request string) string {
	parsed, _ := url.Parse(request)
	queryValue, _ := url.ParseQuery(parsed.RawQuery)
	query := queryValue.Get("item")
	return query
}

func isDynamic(request string) bool {
	parsed, _ := url.Parse(request)
	queryValue, _ := url.ParseQuery(parsed.RawQuery)
	for _, v := range queryValue {
		for _, j := range v {
			if strings.Contains(j, "_") {
				return false
			}
		}
	}
	return true
}

func hashRequest(request string) string {
	hash := crypto.MD5.New()
	hash.Write([]byte(request))
	res := hash.Sum(nil)
	return hex.EncodeToString(res)
}

