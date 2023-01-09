package main

import (
	"redisInAction/Chapter01/model"
	"redisInAction/redisConn"
	"redisInAction/utils"
	"testing"
)

func Test(t *testing.T) {
	conn := redisConn.ConnectRedis()
	client := model.NewArticleRepo(conn)

	articleId := client.PostArticle("username", "A title", "http://www.google.com")
	t.Log("We posted a new article with id: ", articleId)
	utils.AssertStringResult(t, "1", articleId)

	r := client.Conn.HGetAll("article:" + articleId).Val()
	t.Log("\nIts HASH looks like: ", r)
	utils.AssertTrue(t, len(r) != 0)

	client.ArticleVote("article:"+articleId, "other_user")
	v, _ := client.Conn.HGet("article:"+articleId, "votes").Int()
	t.Log("\nWe voted for the article, it now has votes: ", v)
	utils.AssertTrue(t, v >= 1)

	t.Log("\nThe currently highest-scoring articles are: ")
	articles := client.GetArticles(1, "")
	utils.AssertTrue(t, len(articles) >= 1)
	for k, v := range articles {
		t.Log(k, v)
	}

	client.AddRemoveGroups(articleId, []string{"new-group"}, []string{})
	articles = client.GetGroupArticles("new-group", "score:", 1)
	t.Log("\nWe added the article to a new group, other articles include: ")
	utils.AssertTrue(t, len(articles) >= 1)
	for k, v := range articles {
		t.Log(k, v)
	}
	defer client.Reset()
}
