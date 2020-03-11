package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
	"log"
	"math"
	"redisInAction/Chapter08/common"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func (c *Client) CreateUser(login, name string) string {
	llogin := strings.ToLower(login)
	lock := c.AcquireLockWithTimeout("user:"+llogin, 10, 10)
	defer c.ReleaseLock("user:"+llogin, lock)

	if lock == "" {
		return ""
	}

	if c.Conn.HGet("users:", llogin).Val() != "" {
		return ""
	}

	id := c.Conn.Incr("user:id:").Val()

	pipeline := c.Conn.TxPipeline()
	pipeline.HSet("users:", llogin, id)
	pipeline.HMSet(fmt.Sprintf("user:%s", strconv.Itoa(int(id))), "login", login,
		"id", id, "name", name, "followers", 0, "following", 0, "posts", 0,
		"signup", time.Now().UnixNano())
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in CreateUser: ", err)
		return ""
	}
	return strconv.Itoa(int(id))
}

func (c *Client) CreateStatus(uid, message string, data map[string]interface{}) string {
	pipeline := c.Conn.TxPipeline()
	pipeline.HGet(fmt.Sprintf("user:%s", uid), "login")
	pipeline.Incr("status:id:")
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in CreateStatus: ", err)
		return ""
	}
	login, id := res[0].(*redis.StringCmd).Val(), res[1].(*redis.IntCmd).Val()

	if login == "" {
		return ""
	}

	data["message"] = message
	data["posted"] = time.Now().UnixNano()
	data["id"] = id
	data["uid"] = uid
	data["login"] = login

	pipeline.HMSet(fmt.Sprintf("status:%s", strconv.Itoa(int(id))), data)
	pipeline.HIncrBy(fmt.Sprintf("user:%s", uid), "posts", 1)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in CreateStatus: ", err)
		return ""
	}
	return strconv.Itoa(int(id))
}

func (c *Client) PostStatus(uid, message string, data map[string]interface{}) string {
	id := c.CreateStatus(uid, message, data)
	if id == "" {
		return ""
	}

	posted, err := c.Conn.HGet(fmt.Sprintf("status:%s", id), "posted").Float64()
	if err != nil {
		log.Printf("hget from status:%s err: %v\n", id, err)
		return ""
	}
	if posted == 0 {
		return ""
	}

	post := redis.Z{Member: id, Score: posted}
	c.Conn.ZAdd(fmt.Sprintf("profile:%s", uid), &post)

	c.SyndicateStatus(uid, post, 0)
	return id
}

func (c *Client) SyndicateStatus(uid string, post redis.Z, start int) {
	followers := c.Conn.ZRangeByScoreWithScores(fmt.Sprintf("followers:%s", uid),
		&redis.ZRangeBy{Min: "0", Max: "inf", Offset: int64(start), Count: common.Postperpass}).Val()

	pipeline := c.Conn.TxPipeline()
	for i, z := range followers {
		follower := z.Member.(string)
		start = i + 1
		pipeline.ZAdd(fmt.Sprintf("home:%s", follower), &post)
		pipeline.ZRemRangeByRank(fmt.Sprintf("home:%s", follower), 0, -common.Hometimelinesize-1)
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in syndicateStatus: ", err)
		return
	}

	if len(followers) >= int(common.Postperpass) {
		c.executeLater("default", "SyndicateStatus", uid, post, start)
	}
}

func (c *Client) SyndicateStatusList(uid string, post map[string]float64, start int, onLists bool) {
	key := fmt.Sprintf("followers:%s", uid)
	if onLists {
		key = fmt.Sprintf("list:out:%s", uid)
	}
	followers := c.Conn.ZRangeByScoreWithScores(key,
		&redis.ZRangeBy{Min: "0", Max: "inf", Offset: int64(start), Count: common.Postperpass}).Val()

	pipeline := c.Conn.Pipeline()
	var base string
	for i, z := range followers {
		follower := z.Member.(string)
		start = i + 1
		if onLists {
			base = fmt.Sprintf("list:statuses:%s", follower)
		} else {
			base = fmt.Sprintf("home:%s", follower)
		}
		pipeline.ZAdd(base, &redis.Z{Member: follower, Score: post[follower]})
		pipeline.ZRemRangeByRank(base, 0, -common.Hometimelinesize-1)
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in SyndicateStatusList: ", err)
		return
	}

	if len(followers) >= int(common.Postperpass) {
		c.executeLater("default", "SyndicateStatusList", uid, post, start, onLists)
	} else if !onLists {
		c.executeLater("default", "SyndicateStatusList", uid, post, 0, true)
	}
}

func (c *Client) executeLater(queue, name string, args ...interface{}) {
	go func() {
		methodValue := reflect.ValueOf(c).MethodByName(name)
		methodArgs := make([]reflect.Value, 0, len(args))
		for _, v := range args {
			value := reflect.ValueOf(v).Interface()
			methodArgs = append(methodArgs, reflect.ValueOf(value))
		}
		methodValue.Call(methodArgs)
	}()
	time.Sleep(100 * time.Millisecond)
}

func (c *Client) GetStatusMessage(uid, timeline string, page, count int64) []map[string]string {
	statuses := c.Conn.ZRevRange(fmt.Sprintf("%s:%s", timeline, uid),
		(page-1)*count, page*count-1).Val()
	pipeline := c.Conn.TxPipeline()
	for _, id := range statuses {
		pipeline.HGetAll(fmt.Sprintf("status:%s", id))
	}
	res, err := pipeline.Exec()
	if err != nil {
		return nil
	}
	final := make([]map[string]string, 0, len(res))

	for _, val := range res {
		temp := val.(*redis.StringStringMapCmd).Val()
		if len(temp) != 0 {
			final = append(final, temp)
		}
	}
	return final
}

func (c *Client) DeleteStatus(uid, statusId string) bool {
	key := fmt.Sprintf("status:%s", statusId)
	lock := c.AcquireLockWithTimeout(key, 1, 100)
	if lock == "" {
		return false
	}

	if c.Conn.HGet(key, "uid").Val() != uid {
		return false
	}

	pipeline := c.Conn.TxPipeline()
	pipeline.Del(key)
	pipeline.ZRem(fmt.Sprintf("profile:%s", uid), statusId)
	pipeline.ZRem(fmt.Sprintf("home:%s", uid), statusId)
	pipeline.HIncrBy(fmt.Sprintf("user:%s", uid), "posts", -1)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in DeleteStatus: ", err)
		return false
	}
	defer c.ReleaseLock(key, lock)
	return true
}

func (c *Client) CleanTimeLines(uid, statusId string, start int, onLists bool) {
	var key, base string
	if onLists {
		key = fmt.Sprintf("list:out:%s", uid)
	} else {
		key = fmt.Sprintf("followers:%s", uid)
	}
	followers := c.Conn.ZRangeByScoreWithScores(key,
		&redis.ZRangeBy{Min: "0", Max: "inf", Offset: int64(start), Count: common.Postperpass}).Val()

	pipeline := c.Conn.Pipeline()

	for _, z := range followers {
		follower := z.Member.(string)
		if onLists {
			base = fmt.Sprintf("list:statuses:%s", follower)
		} else {
			base = fmt.Sprintf("home:%s", follower)
		}
		pipeline.ZRem(base, statusId)
	}

	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in CleanTimeLines: ", err)
		return
	}

	if len(followers) >= int(common.Postperpass) {
		c.executeLater("default", "CleanTimeLines", uid, statusId, start, onLists)
	} else if !onLists {
		c.executeLater("default", "CleanTimeLines", uid, statusId, 0, true)
	}
}

func (c *Client) FollowUser(uid, otherUid string) bool {
	fkey1 := fmt.Sprintf("following:%s", uid)
	fkey2 := fmt.Sprintf("followers:%s", otherUid)

	if c.Conn.ZScore(fkey1, otherUid).Val() != 0 {
		return false
	}

	now := time.Now().UnixNano()

	pipeline := c.Conn.TxPipeline()
	pipeline.ZAdd(fkey1, &redis.Z{Member: otherUid, Score: float64(now)})
	pipeline.ZAdd(fkey2, &redis.Z{Member: uid, Score: float64(now)})
	pipeline.ZRevRangeWithScores(fmt.Sprintf("profile:%s", otherUid), 0, common.Hometimelinesize-1)
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in FollowUser")
	}
	following, followers, statusAndScore :=
		res[0].(*redis.IntCmd).Val(), res[1].(*redis.IntCmd).Val(), res[2].(*redis.ZSliceCmd).Val()

	pipeline.HIncrBy(fmt.Sprintf("user:%s", uid), "following", following)
	pipeline.HIncrBy(fmt.Sprintf("user:%s", otherUid), "followers", followers)
	if statusAndScore != nil {
		for _, z := range statusAndScore {
			pipeline.ZAdd(fmt.Sprintf("home:%s", uid), &z)
		}
	}
	pipeline.ZRemRangeByRank(fmt.Sprintf("home:%s", uid), 0, -common.Hometimelinesize-1)

	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in FollowUser")
		return false
	}
	return true
}

func (c *Client) UnfollowUser(uid, otherUid string) bool {
	fkey1 := fmt.Sprintf("following:%s", uid)
	fkey2 := fmt.Sprintf("followers:%s", otherUid)

	if c.Conn.ZScore(fkey1, otherUid).Val() == 0 {
		return false
	}

	pipeline := c.Conn.TxPipeline()
	pipeline.ZRem(fkey1, otherUid)
	pipeline.ZRem(fkey2, uid)
	pipeline.ZRevRange(fmt.Sprintf("profile:%s", otherUid), 0, common.Hometimelinesize-1)
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in FollowUser")
	}
	following, followers, status :=
		res[0].(*redis.IntCmd).Val(), res[1].(*redis.IntCmd).Val(), res[2].(*redis.StringSliceCmd).Val()

	pipeline.HIncrBy(fmt.Sprintf("user:%s", uid), "following", -following)
	pipeline.HIncrBy(fmt.Sprintf("user:%s", otherUid), "followers", -followers)
	if len(status) != 0 {
		pipeline.ZRem(fmt.Sprintf("home:%s", uid), status)
	}

	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in UnfollowUser")
		return false
	}
	return true
}

func (c *Client) RefillTimeline(incoming, timeline string, start int) {
	if start == 0 && c.Conn.ZCard(timeline).Val() >= 750 {
		return
	}

	users := c.Conn.ZRangeByScoreWithScores(incoming,
		&redis.ZRangeBy{Min: "0", Max: "inf", Offset: int64(start), Count: common.REFILLUSERSSTEP}).Val()

	pipeline := c.Conn.TxPipeline()

	for i, z := range users {
		start = i + 1
		pipeline.ZRevRangeWithScores(fmt.Sprintf("profile:%s", z.Member),
			0, common.Hometimelinesize-1)
	}

	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in RefillTimeline", err)
		return
	}

	message := make([]*redis.Z, 0, len(res))
	for _, z := range res[0].(*redis.ZSliceCmd).Val() {
		temp := z
		message = append(message, &temp)
	}
	sort.Slice(message, func(i, j int) bool {
		return message[i].Score < message[j].Score
	})
	message = message[:common.Hometimelinesize]

	pipeline = c.Conn.TxPipeline()
	if len(message) != 0 {
		pipeline.ZAdd(timeline, message...)
	}
	pipeline.ZRemRangeByRank(timeline, 0, -common.Hometimelinesize-1)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in RefillTimeline")
		return
	}

	if len(users) >= common.REFILLUSERSSTEP {
		c.executeLater("default", "RefillTimeline", incoming, timeline, start)
	}

}

func (c *Client) AcquireLockWithTimeout(lockname string, acquireTimeout, lockTimeout float64) string {
	identifier := uuid.NewV4().String()
	lockname = "lock:" + lockname
	finalLockTimeout := math.Ceil(lockTimeout)

	end := time.Now().UnixNano() + int64(acquireTimeout*1e9)
	for time.Now().UnixNano() < end {
		if c.Conn.SetNX(lockname, identifier, 0).Val() {
			c.Conn.Expire(lockname, time.Duration(finalLockTimeout)*time.Second)
			return identifier
		} else if c.Conn.TTL(lockname).Val() < 0 {
			c.Conn.Expire(lockname, time.Duration(finalLockTimeout)*time.Second)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func (c *Client) ReleaseLock(lockname, identifier string) bool {
	lockname = "lock:" + lockname
	var flag = true
	for flag {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			pipe := tx.TxPipeline()
			if tx.Get(lockname).Val() == identifier {
				pipe.Del(lockname)
				if _, err := pipe.Exec(); err != nil {
					return err
				}
				flag = true
				return nil
			}

			tx.Unwatch()
			flag = false
			return nil
		})

		if err != nil {
			log.Println("watch failed in ReleaseLock, err is: ", err)
			return false
		}
	}
	return true
}
