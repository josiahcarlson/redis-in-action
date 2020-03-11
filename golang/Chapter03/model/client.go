package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"log"
	"redisInAction/Chapter03/common"
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
func (r *Client) Reset() {
	r.Conn.FlushDB()
}

func (r *Client) NotRans() {
	fmt.Println(r.Conn.Incr("notrans: ").Val())
	time.Sleep(100 * time.Millisecond)
	fmt.Println(r.Conn.Decr("notrans: ").Val())
}

func (r *Client) Trans() {
	pipeline := r.Conn.Pipeline()
	pipeline.Incr("trans:")
	time.Sleep(100 * time.Millisecond)
	pipeline.Decr("trans:")
	_, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline failed, the err is: ", err)
	}
}

func (r *Client) Publisher(n int) {
	time.Sleep(1 * time.Second)
	for n > 0 {
		r.Conn.Publish("channel", n)
		n--
	}
}

func (r *Client) RunPubsub() {
	pubsub := r.Conn.Subscribe("channel")
	defer pubsub.Close()

	var count int32 = 0
	for item := range pubsub.Channel() {
		fmt.Println(item.String())
		atomic.AddInt32(&count, 1)
		fmt.Println(count)

		switch count {
		case 4:
			if err := pubsub.Unsubscribe("channel"); err != nil {
				log.Println("unsubscribe faile, err: ", err)
			} else {
				fmt.Println("unsubscribe success")
			}
		case 5:
			break
		default:
		}
	}
}

func (r *Client) UpdateToken(token, user, item string) {
	timestamp := time.Now().Unix()
	r.Conn.HSet("login:", token, user)
	r.Conn.ZAdd("recent", &redis.Z{Score: float64(timestamp), Member: token})
	if item != "" {
		key := "viewed" + token
		r.Conn.LRem(key, 1, item)
		r.Conn.RPush(key, item)
		r.Conn.LTrim(key, -25, -1)
		r.Conn.ZIncrBy("viewed:", -1, item)
	}
}

func (r *Client) ArticleVote(article, user string) {
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	posted := r.Conn.ZScore("time", article).Val()
	if posted < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	pipeline := r.Conn.Pipeline()
	pipeline.SAdd("voted:"+articleId, user)
	pipeline.Expire("voted:"+articleId, time.Duration(int(posted-float64(cutoff)))*time.Second)
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline failed, the err is: ", err)
	}
	if res[0] != nil {
		pipeline.ZIncrBy("score:", common.VoteScore, article)
		r.Conn.HIncrBy(article, "votes", 1)
		if _, err := pipeline.Exec(); err != nil {
			log.Println("pipeline failed, the err is: ", err)
		}
	}
}

func (r *Client) GetArticles(page int64, order string) []map[string]string {
	if order == "" {
		order = "score:"
	}
	start := utils.Max(page-1, 0) * common.ArticlesPerPage
	end := start + common.ArticlesPerPage - 1

	ids := r.Conn.ZRevRange(order, start, end).Val()
	articles := []map[string]string{}
	for _, id := range ids {
		articleData := r.Conn.HGetAll(id).Val()
		articleData["id"] = id
		articles = append(articles, articleData)
	}
	return articles
}

func (r *Client) CheckToken(token string) string {
	return r.Conn.Get("login:" + token).String()
}

func (r *Client) AddToCart(session, item string, count int) {
	switch {
	case count <= 0:
		r.Conn.HDel("cart:"+session, item)
	default:
		r.Conn.HSet("cart:"+session, item, count)
	}
	r.Conn.Expire("cart:"+session, common.THIRTYDAYS)
}

func (r *Client) UpdateTokenCh3(token, user, item string) {
	r.Conn.Set("login:"+token, user, common.THIRTYDAYS)
	key := "viewed:" + token
	if item != "" {
		r.Conn.LRem(key, 1, item)
		r.Conn.RPush(key, item)
		r.Conn.LTrim(key, -25, -1)
		r.Conn.ZIncrBy("viewed:", -1, item)
	}
	r.Conn.Expire(key, common.THIRTYDAYS)
}
