package model

import (
	"github.com/go-redis/redis/v7"
	"log"
	"redisInAction/Chapter01/common"
	"strconv"
	"strings"
	"time"
)

type Article interface {
	ArticleVote(string, string)
	PostArticle(string, string, string) string
	GetArticles(int64, string) []map[string]string
	AddRemoveGroups(string, []string, []string)
	GetGroupArticles(string, string, int64) []map[string]string
	Reset()
}

type ArticleRepo struct {
	Conn *redis.Client
}

func NewArticleRepo(conn *redis.Client) *ArticleRepo {
	return &ArticleRepo{Conn: conn}
}

func (r *ArticleRepo) ArticleVote(article, user string) {
	cutoff := time.Now().Unix() - common.OneWeekInSeconds
	if r.Conn.ZScore("time", article).Val() < float64(cutoff) {
		return
	}

	articleId := strings.Split(article, ":")[1]
	if r.Conn.SAdd("voted:"+articleId, user).Val() != 0 {
		r.Conn.ZIncrBy("score:", common.VoteScore, article)
		r.Conn.HIncrBy(article, "votes", 1)
	}
}

func (r *ArticleRepo) PostArticle(user, title, link string) string {
	articleId := strconv.Itoa(int(r.Conn.Incr("article:").Val()))

	voted := "voted:" + articleId
	r.Conn.SAdd(voted, user)
	r.Conn.Expire(voted, common.OneWeekInSeconds*time.Second)

	now := time.Now().Unix()
	article := "article:" + articleId
	r.Conn.HMSet(article, map[string]interface{}{
		"title":  title,
		"link":   link,
		"poster": user,
		"time":   now,
		"votes":  1,
	})

	r.Conn.ZAdd("score:", &redis.Z{Score: float64(now + common.VoteScore), Member: article})
	r.Conn.ZAdd("time", &redis.Z{Score: float64(now), Member: article})
	return articleId
}

func (r *ArticleRepo) GetArticles(page int64, order string) []map[string]string {
	if order == "" {
		order = "score:"
	}
	start := (page - 1) * common.ArticlesPerPage
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

func (r *ArticleRepo) AddRemoveGroups(articleId string, toAdd, toRemove []string) {
	article := "article:" + articleId
	for _, group := range toAdd {
		r.Conn.SAdd("group:"+group, article)
	}
	for _, group := range toRemove {
		r.Conn.SRem("group:"+group, article)
	}
}

func (r *ArticleRepo) GetGroupArticles(group, order string, page int64) []map[string]string {
	if order == "" {
		order = "score:"
	}
	key := order + group
	if r.Conn.Exists(key).Val() == 0 {
		res := r.Conn.ZInterStore(key, &redis.ZStore{Aggregate: "MAX", Keys:[]string{"group:"+group, order}}).Val()
		if res <= 0 {
			log.Println("ZInterStore return 0")
		}
	}
	r.Conn.Expire(key, 60*time.Second)
	return r.GetArticles(page, key)
}

func (r *ArticleRepo) Reset() {
	r.Conn.FlushDB()
}
