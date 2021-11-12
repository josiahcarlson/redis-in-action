package model

import (
	"fmt"
	"log"
	"redisInAction/Chapter07/common"
	"redisInAction/utils"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func Tokenize(content string) []string {
	words := utils.Set{}
	for _, match := range common.WORDSRE.FindAllString(strings.ToLower(content), -1) {
		// 至少兩個字符長度的單字
		if len(match) < 2 {
			continue
		}
		words.Add(match)
	}
	return words.Intersection(&common.STOPWORDS)
}

// IndexDocument : Listing 7.1 Functions to tokenize and index a document
// 建立set("idx:"+word)然後將 docid 放入此 set
func (c *Client) IndexDocument(docid, content string) int64 {
	words := Tokenize(content)

	pipeline := c.Conn.TxPipeline()
	// 建立單字的set, 裡面記錄有哪幾個文件出現過
	for _, word := range words {
		pipeline.SAdd("idx:"+word, docid)
	}
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in IndexDocument: ", err)
		return 0
	}
	return int64(len(res))
}

// setCommon : 對集合做交集, 連集, 差集運算 (SInterStore, SUnionStore, SDiffStore)
// 回傳運算結果的set id
func (c *Client) setCommon(method string, names *[]string, ttl int) string {
	// 建立臨時的 id, 給臨時的set使用
	id := uuid.NewV4().String()
	pipeline := c.Conn.TxPipeline()

	namelist := make([]reflect.Value, 0, len(*names)+1)
	namelist = append(namelist, reflect.ValueOf("idx:"+id))
	for _, name := range *names {
		namelist = append(namelist, reflect.ValueOf("idx:"+name))
	}

	// SInterStore(destination, key…)：將兩個表交集/並集/補集元素copy到第三個, ex: client.SInterStore("sdemo2", key, key1).Val()
	methodValue := reflect.ValueOf(pipeline).MethodByName(method)
	methodValue.Call(namelist)

	// 告訴 redis 時間到自動刪除此集合
	pipeline.Expire("idx:"+id, time.Duration(ttl)*time.Second)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in setCommon: ", err)
		return ""
	}
	return id
}

func (c *Client) Intersect(items []string, ttl int) string {
	return c.setCommon("SInterStore", &items, ttl)
}

func (c *Client) Union(items []string, ttl int) string {
	return c.setCommon("SUnionStore", &items, ttl)
}

func (c *Client) Difference(items []string, ttl int) string {
	return c.setCommon("SDiffStore", &items, ttl)
}

// Parse : Listing 7.3 A function for parsing a search query
// query :="test +query without -stopwords" => all: [][]string{{"test", "query"}, {"without"}}, unwantedlist : []string{"stopwords"}
func Parse(query string) (all [][]string, unwantedlist []string) {
	unwanted, current := utils.Set{}, utils.Set{}
	// 將所有單字找出
	for _, word := range common.QUERYRE.FindAllString(strings.ToLower(query), -1) {
		prefix := word[0]
		if prefix == '+' || prefix == '-' {
			word = word[1:]
		} else {
			prefix = 0
		}

		switch {
		case len(word) < 2 || common.STOPWORDS[sort.SearchStrings(common.STOPWORDS, word)] == word:
			continue
		case prefix == '-':
			// 不需要的單詞, 添加到不需要單詞的set裡
			unwanted.Add(word)
			continue
		case len(current) != 0 && prefix == 0:
			all = append(all, current.Getkeys())
			current = utils.Set{}
		}

		current.Add(word)
	}

	// 把剩餘的單詞都放在
	if len(current) != 0 {
		all = append(all, current.Getkeys())
	}

	unwantedlist = append(unwantedlist, unwanted.Getkeys()...)
	return
}

// ParseAndSearch : Listing 7.4 A function to parse a query and search documents
// 回傳 臨時的set id
func (c *Client) ParseAndSearch(query string, ttl int) string {
	all, unwanted := Parse(query)

	if len(all) == 0 {
		return ""
	}

	toIntersect := []string{}
	for _, syn := range all {
		// 找尋各個同義詞的arrray
		if len(syn) > 1 {
			// 如果同義詞包的單字超過一個, 執行聯集運算
			toIntersect = append(toIntersect, c.Union(syn, ttl))
		} else {
			// 如果同義詞包的單字只有一個, 直接使用此單字
			toIntersect = append(toIntersect, syn[0])
		}
	}

	var intersectResult string
	if len(toIntersect) > 1 {
		intersectResult = c.Intersect(toIntersect, ttl)
	} else {
		intersectResult = toIntersect[0]
	}

	if len(unwanted) != 0 {
		unwanted = append([]string{intersectResult}, unwanted...)
		return c.Difference(unwanted, ttl)
	}

	return intersectResult
}

// SearchAndSort : Listing 7.5 A function to parse and search, sorting the results
// client.SearchAndSort("content", "", 300, "-id", 0, 20)
func (c *Client) SearchAndSort(query string, id string, ttl int, sort string, start, num int64) ([]string, string) {
	var order string
	if strings.HasPrefix(sort, "-") {
		order = "DESC"
	} else {
		order = "ASC"
	}

	// 把字首字串去掉
	sort = strings.TrimPrefix(sort, "-")
	by := "kb:doc:*->" + sort

	// 告訴 redis 排序是用數值方式進行,或者是字母
	alpha := !strings.Contains(sort, "updated") && !strings.Contains(sort, "id") &&
		!strings.Contains(sort, "created")

	// 如果用戶給定已有的搜尋結果, 並且還沒過期, 延長此結果
	if id != "" && !c.Conn.Expire(id, time.Duration(ttl)*time.Second).Val() {
		id = ""
	}

	if id == "" {
		id = c.ParseAndSearch(query, ttl)
	}

	var res *redis.StringSliceCmd
	pipeline := c.Conn.TxPipeline()
	// 獲取集合元素個數
	pipeline.SCard("idx:" + id)
	res = pipeline.Sort("idx:"+id, &redis.Sort{By: by, Alpha: alpha, Order: order, Offset: start, Count: num})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in SearchAndSort: ", err)
		return nil, ""
	}

	return res.Val(), id
}

// SearchAndZsort : Listing 7.6 An updated function to search and sort based on votes and updated times
// 與SearchAndSort相似, 但多了基於更新時間, 投票數量或者同時基於兩者條件進行排序收尋
func (c *Client) SearchAndZsort(query string, id string, ttl int, update, vote float64, start, num int64,
	desc bool) ([]string, string) {
	// 刷新已有搜尋結果的ttl
	if id != "" && !c.Conn.Expire(id, time.Duration(ttl)*time.Second).Val() {
		id = ""
	}

	if id == "" {
		id = c.ParseAndSearch(query, ttl)

		// 再計算交集時也會用到傳入的ID鍵, 但這個鍵部會被用到排序權重
		// zstore.Keys = append(zstore.Keys, "idx:"+key)
		// zstore.Weights = append(zstore.Weights, scores[key])
		scoredSearch := map[string]float64{
			id:            0,
			"sort:update": update,
			"sort:votes":  vote,
		}
		id = c.Zintersect(scoredSearch, ttl, "")
	}

	pipeline := c.Conn.TxPipeline()
	pipeline.ZCard("idx:" + id)
	var res *redis.StringSliceCmd
	// 從搜尋結果取出一頁
	if desc {
		res = pipeline.ZRevRange("idx:"+id, start, start+num-1)
	} else {
		res = pipeline.ZRange("idx:"+id, start, start+num-1)
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in SearchAndZsort: ", err)
		return nil, ""
	}

	return res.Val(), id
}

// zsetCommon : Listing 7.7 Some helper functions for performing ZSET intersections and unions
func (c *Client) zsetCommon(method string, scores map[string]float64, ttl int, Aggre string) string {
	id := uuid.NewV4().String()
	pipeline := c.Conn.TxPipeline()

	zstore := redis.ZStore{}
	for key := range scores {
		zstore.Keys = append(zstore.Keys, "idx:"+key)
		zstore.Weights = append(zstore.Weights, scores[key])
	}
	switch strings.ToLower(Aggre) {
	case "max":
		zstore.Aggregate = "MAX"
	case "min":
		zstore.Aggregate = "MIN"
	default:
		zstore.Aggregate = "SUM"
	}
	methodValue := reflect.ValueOf(pipeline).MethodByName(method)
	args := []reflect.Value{reflect.ValueOf("idx:" + id), reflect.ValueOf(&zstore)}
	methodValue.Call(args)
	pipeline.Expire("idx:"+id, time.Duration(ttl)*time.Second)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in zsetCommon: ", err)
		return ""
	}
	return id
}

func (c *Client) Zintersect(items map[string]float64, ttl int, Aggre string) string {
	return c.zsetCommon("ZInterStore", items, ttl, Aggre)
}

func (c *Client) ZUnion(items map[string]float64, ttl int, Aggre string) string {
	return c.zsetCommon("ZUnionStore", items, ttl, Aggre)
}

// StringToScore : 將字串轉成為數字分值
func (c *Client) StringToScore(str string, ignoreCase bool) int {
	if ignoreCase {
		str = strings.ToLower(str)
	}

	byteString := make([]byte, 0, 6)
	if len(str) < 6 {
		byteString = []byte(str)
	} else {
		byteString = []byte(str[:6])
	}
	pieces := make([]int, 0, len(byteString))
	for _, v := range byteString {
		pieces = append(pieces, int(v))
	}
	for len(pieces) < 6 {
		// 為長度不足6個字符的字串貼加佔位符, -1加在string末尾
		pieces = append(pieces, -1)
	}

	score, temp := 0, 0
	for _, piece := range pieces {
		score = score*257 + piece + 1
	}

	if len(str) > 6 {
		temp = 1
	}

	return score*2 + temp
}

// IndexAd : Listing 7.10 A method for indexing an ad that’s targeted on location and ad content
// client.IndexAd("1", []string{"USA", "CA"}, common.CONTENT, "cpc", 0.25)
// client.IndexAd("2", []string{"USA", "VA"}, common.CONTENT+"wooooo", "cpc", 0.125)
// 1. The first is that an ad can actually have multiple targeted locations.
// 2. The second is that we’ll keep a dictionary that holds information about the average number of clicks and actions across the entire system
// 3. Finally, we’ll also keep a SET of all of the terms that we can optionally target in the ad. I include this information as a precursor to learning about user behavior a little later.
func (c *Client) IndexAd(id string, locations []string, content, types string, value float64) {
	pipeline := c.Conn.TxPipeline()
	for _, location := range locations {
		pipeline.SAdd("idx:req:"+location, id)
	}

	words := Tokenize(content)
	for _, word := range words {
		pipeline.ZAdd("idx:"+word, &redis.Z{Member: id, Score: 0})
	}

	if common.AVERAGEPER1K[types] == 0 {
		common.AVERAGEPER1K[types] = 1
	}

	rvalue := toECPM(types, 1000, common.AVERAGEPER1K[types], value)
	pipeline.HSet("type:", id, types)

	// 講廣告的 eCPM兼夾到一個記錄所有廣告的ePCM的zset裝
	pipeline.ZAdd("idx:ad:value:", &redis.Z{Member: id, Score: rvalue})

	// 將廣告的基本價個添加到一個記錄所有廣告的基本價格的zset裡面
	pipeline.ZAdd("ad:baseValue:", &redis.Z{Member: id, Score: value})

	//pipeline.SAdd("terms:"+id, words) FLAG
	for _, word := range words {
		// 將能夠對廣告進行定向的單字全部記錄起來
		pipeline.SAdd("terms:"+id, word)
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in IdexAd: ", err)
	}
}

func cpcToEcpm(views, clicks, cpc float64) float64 {
	return 1000 * cpc * clicks / views
}

func cpaToEcpm(views, actions, cpa float64) float64 {
	return 1000 * cpa * actions / views
}

func toECPM(types string, views, avg, value float64) float64 {
	var rvalue float64
	switch types {
	case "cpc":
		rvalue = cpcToEcpm(views, avg, value)
	case "cpa":
		rvalue = cpaToEcpm(views, avg, value)
	case "cpm":
		rvalue = value
	}
	return rvalue
}

// TargetAds : 通過位置和頁面內容附加值實現廣告定向操作
func (c *Client) TargetAds(locations []string, content string) (string, string) {
	matchedAds, baseEcpm := c.matchLocation(&locations)
	// fmt.Printf("matchedAds:%s,baseEcpm:%s", matchedAds, baseEcpm)

	// Get an ID that can be used for reporting and recording of this particular ad targe.
	words, targetedAds := c.finishScoring(matchedAds, baseEcpm, content)

	pipeline := c.Conn.TxPipeline()
	tempAd := &redis.StringSliceCmd{}
	targetId := &redis.IntCmd{}
	targetId = pipeline.Incr("ads:served:")

	// 找到eCPM最高的廣告, 並獲得此廣告 ID. [{Score:0.25 Member:1} {Score:0.125 Member:2}]
	tempAd = pipeline.ZRevRange("idx:"+targetedAds, 0, 0)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in TargetAds: ", err)
	}

	targetedAd := tempAd.Val()
	if len(targetedAd) == 0 {
		return "", ""
	}

	adId := targetedAd[0]
	// 紀錄一系列定向操作的執行結果, 作為學習用戶行為的其中一步驟
	c.recordTargetingResult(strconv.Itoa(int(targetId.Val())), adId, words)
	return strconv.Itoa(int(targetId.Val())), adId
}

/**
 * @description: 基於位置執行廣告定向操作的輔助函式. A helper function for targeting ads based on location
 * @param {*[]string} locations
 * @return {*}
 */
func (c *Client) matchLocation(locations *[]string) (string, string) {
	var required []string
	for _, loc := range *locations {
		// 根據所有給定的位置, 找出需要執行並集操作的集合鍵 ex: idx:req:USA idx:req:CA
		required = append(required, "req:"+loc)
	}
	// 找出指定地區相匹配的廣告, 並將他們存到set裡
	matchedAds := c.Union(required, 300)

	// 找到存儲著所有被匹配廣告的集合, 交集以及存儲著所有被匹配廣告的基本eCPM的zset, 並返回此zset ID
	baseEcpm := c.Zintersect(map[string]float64{matchedAds: 0, "ad:value:": 1}, 30, "")

	return matchedAds, baseEcpm
}

/**
 * @description: Calculating the eCPM of ads including content match bonuses
 * @param {*} matched
 * @param {*} base
 * @param {string} content
 * @return {*}
 */
func (c *Client) finishScoring(matched, base, content string) ([]string, string) {
	bonusEcpm := map[string]float64{}
	// 對內容進行 tokenize 以便與廣告進行匹配
	words := Tokenize(content)
	for _, word := range words {
		// Find the ads that are location targeted that also have one of the words in the content. 找出那些既未妤定向位置內, 又擁有頁面內容其中一個單字的廣告
		wordBonus := c.Zintersect(map[string]float64{matched: 0, word: 1}, 30, "")
		bonusEcpm[wordBonus] = 1
		// fmt.Println(c.Conn.ZRange("idx:"+wordBonus, 0, -1))
	}

	if len(bonusEcpm) != 0 {
		// Find the minimum and maximum eCPM bonuses for each ad.
		minimum := c.ZUnion(bonusEcpm, 30, "MIN")
		maximum := c.ZUnion(bonusEcpm, 30, "MAX")
		// 將廣告的基本價錢, 最小 eCPM bouns 的一半, 最大 eCPM bouns 的一半相加
		return words, c.ZUnion(map[string]float64{base: 1, minimum: 0.5, maximum: 0.5}, 30, "")
	}
	// If there were no words in the content to match against,return just the known eCPM.
	return words, base
}

/**
 * @description: Listing 7.14 A method for recording the result after we’ve targeted an ad. To record this information, we’ll store a SET of the words that were targeted and keep counts of the number of times that the ad and words were seen as part of a single ZSET per ad.
 * @param {*} targetId
 * @param {string} adId
 * @param {[]string} words
 * @return {*}
 */
func (c *Client) recordTargetingResult(targetId, adId string, words []string) {
	pipeline := c.Conn.TxPipeline()
	terms := c.Conn.SMembers("terms:" + adId).Val()
	sort.Slice(terms, func(i, j int) bool {
		return terms[i] < terms[j]
	})
	matched := make([]string, 0, len(words))
	for _, word := range words {
		idx := sort.SearchStrings(terms, word)
		if idx < len(terms) && terms[idx] == word {
			matched = append(matched, word)
		}
	}

	// set 儲存被定向的單字, 如果有匹配的單字出現, 記錄他們, 設定15 mins ttl
	if len(matched) != 0 {
		matchedKey := fmt.Sprintf("terms:matched:%s", targetId)
		for _, word := range matched {
			pipeline.SAdd(matchedKey, word)
		}
		pipeline.Expire(matchedKey, 900*time.Second)
	}

	// 為每種類型的廣告分別紀錄他觀看次數
	types := c.Conn.HGet("type:", adId).Val()
	pipeline.Incr(fmt.Sprintf("type:%s:views:", types))

	// 紀錄廣告以及廣告包含的單字的展示訊息
	for _, word := range matched {
		pipeline.ZIncrBy(fmt.Sprintf("views:%s", adId), 1, word)
	}
	pipeline.ZIncrBy(fmt.Sprintf("views:%s", adId), 1, "")

	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in recordTargetingResult : ", err)
		return
	}

	// 廣告每展示100次就更新一次他的ePCM, 抓res的最後一筆(zset: views:adId)
	if int64(res[len(res)-1].(*redis.FloatCmd).Val())%100 == 0 {
		c.UpdateCpms(adId)
	}
}

func (c *Client) UpdateCpms(adId string) {
	pipeline := c.Conn.TxPipeline()
	pipeline.HGet("type:", adId)
	pipeline.ZScore("ad:baseValue:", adId)
	pipeline.SMembers("terms:" + adId)
	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline1 err in UpdateCpms: ", err)
		return
	}
	types := res[0].(*redis.StringCmd).Val()
	baseValue := res[1].(*redis.FloatCmd).Val()
	words := res[2].(*redis.StringSliceCmd).Val()

	which := "clicks"
	if types == "cpa" {
		which = "actions"
	}

	pipeline.Get(fmt.Sprintf("type:%s:views:", types))
	pipeline.Get(fmt.Sprintf("type:%s:%s:", types, which))
	res, err = pipeline.Exec()
	if err != nil && err != redis.Nil {
		log.Println("pipeline err in UpdateCpms: ", err)
	}
	typeViews, _ := res[0].(*redis.StringCmd).Int()
	typeClicks, _ := res[1].(*redis.StringCmd).Int()

	if typeViews == 0 {
		typeViews = 1
	}

	if typeClicks == 0 {
		typeClicks = 1
	}
	common.AVERAGEPER1K[types] = float64(1000*typeClicks) / float64(typeViews)

	if types == "cpm" {
		return
	}

	viewKey := fmt.Sprintf("views:%s", adId)
	clickKey := fmt.Sprintf("%s:%s", which, adId)

	pipeline.ZScore(viewKey, "")
	pipeline.ZScore(clickKey, "")
	res, err = pipeline.Exec()
	if err != nil && err != redis.Nil {
		log.Println("pipeline2 err in UpdateCpms: ", err)
		//return
	}
	var adViews, adClicks float64
	if err == redis.Nil {
		adViews, adClicks = 0, 0
	} else {
		adViews, adClicks = res[0].(*redis.FloatCmd).Val(), res[1].(*redis.FloatCmd).Val()
	}

	if adViews == 0 {
		adViews = 1
	}

	var adEcpm float64
	if adClicks < 1 {
		adEcpm = c.Conn.ZScore("idx:ad:value:", adId).Val()
	} else {
		adEcpm = toECPM(types, adViews, adClicks, baseValue)
		pipeline.ZAdd("idx:ad:value:", &redis.Z{Member: adId, Score: adEcpm})
	}

	for _, word := range words {
		pipeline.ZScore(viewKey, word)
		pipeline.ZScore(clickKey, word)
		res, err = pipeline.Exec()
		if err != nil && err != redis.Nil {
			log.Println("pipeline3 err in UpdateCpms: ", err)
			return
		}
		var views, clicks float64
		if err == redis.Nil {
			adViews, adClicks = 0, 0
		} else {
			views, clicks = res[len(res)-2].(*redis.FloatCmd).Val(), res[len(res)-1].(*redis.FloatCmd).Val()
		}

		if clicks < 1 {
			continue
		}

		if views == 0 {
			views = 1
		}

		wordEcpm := toECPM(types, views, clicks, baseValue)
		bonus := wordEcpm - adEcpm
		pipeline.ZAdd("idx:"+word, &redis.Z{Member: adId, Score: bonus})
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline4 err in UpdateCpms: ", err)
		return
	}
}

/**
 * @description: Listing 7.15 A method for recording clicks on an ad
 * @param {*} targetId
 * @param {string} adId
 * @param {bool} action
 * @return {*}
 */
func (c *Client) RecordClick(targetId, adId string, action bool) {
	pipeline := c.Conn.TxPipeline()
	clickKey := fmt.Sprintf("clicks:%s", adId)

	matchKey := fmt.Sprintf("terms:matched:%s", targetId)

	types := c.Conn.HGet("type:", adId).Val()
	if types == "cpa" {
		// 如果是一個按動作計費的廣告, 並且被匹配的單字能存在,那麼刷新此但自過期時間
		pipeline.Expire(matchKey, 900*time.Second)
		if action {
			clickKey = fmt.Sprintf("actions:%s", adId)
		}
	}

	// 廣告類型的計數器
	if action && types == "cpa" {
		pipeline.Incr(fmt.Sprintf("type:%s:actions:", types))
	} else {
		pipeline.Incr(fmt.Sprintf("type:%s:clicks:", types))
	}

	matched := c.Conn.SMembers(matchKey).Val()
	matched = append(matched, "")
	for _, word := range matched {
		pipeline.ZIncrBy(clickKey, 1, word)
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in RecordClick: ", err)
		return
	}

	// 對廣告出現的所有單字的 eCPM 進行更新
	c.UpdateCpms(adId)
}

func (c *Client) AddJob(jodId string, requireSkills []string) {
	c.Conn.SAdd("job:"+jodId, requireSkills)
}

func (c *Client) IsQualified(jobId string, candidateSkills []string) []string {
	var res *redis.StringSliceCmd
	temp := uuid.NewV4().String()
	pipline := c.Conn.TxPipeline()
	pipline.SAdd(temp, candidateSkills)
	pipline.Expire(temp, 5*time.Second)
	res = pipline.SDiff("job:"+jobId, temp)
	if _, err := pipline.Exec(); err != nil {
		log.Println("pipeline err in RecordClick: ", err)
		return nil
	}
	return res.Val()
}

func (c *Client) IndexJob(jobId string, skills []string) {
	countSkill := utils.Set{}
	pipeline := c.Conn.TxPipeline()
	for _, skill := range skills {
		pipeline.SAdd("idx:skill:"+skill, jobId)
		countSkill.Add(skill)
	}
	pipeline.ZAdd("idx:jobs:req", &redis.Z{Member: jobId, Score: float64(len(countSkill))})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in RecordClick: ", err)
		return
	}
}

func (c *Client) FindJobs(candidateSkills []string) []string {
	skills := map[string]float64{}
	for _, skill := range candidateSkills {
		skills["skill:"+skill] = 1
	}
	jobScores := c.ZUnion(skills, 30, "")
	finalResult := c.Zintersect(map[string]float64{jobScores: -1, "jobs:req": 1}, 30, "")
	return c.Conn.ZRangeByScore("idx:"+finalResult, &redis.ZRangeBy{Max: "0", Min: "0"}).Val()
}

func (c *Client) IndexJobLevels(jobId string, skillLevels map[string]int64) {
	totalSkills := utils.Set{}
	pipeline := c.Conn.TxPipeline()
	for skill, level := range skillLevels {
		totalSkills.Add(skill)
		level := utils.Min(level, common.SKILLLEVELLIMIT)
		for wlevel := level; wlevel < common.SKILLLEVELLIMIT+1; wlevel++ {
			pipeline.SAdd(fmt.Sprintf("idx:skill:%s:%s", skill, strconv.Itoa(int(wlevel))), jobId)
		}
	}
	pipeline.ZAdd("idx:jobs:req", &redis.Z{Member: jobId, Score: float64(len(totalSkills))})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in IndexJobLevels: ", err)
		return
	}
}

func (c *Client) SearchJobLevels(skillLevels map[string]int64) []string {
	skills := map[string]float64{}
	for skill, level := range skillLevels {
		level = utils.Min(level, common.SKILLLEVELLIMIT)
		skills[fmt.Sprintf("skill:%s:%s", skill, strconv.Itoa(int(level)))] = 1
	}
	jobScores := c.ZUnion(skills, 30, "")
	finalResult := c.Zintersect(map[string]float64{jobScores: -1, "jobs:req": 1}, 30, "")
	return c.Conn.ZRangeByScore("idx:"+finalResult, &redis.ZRangeBy{Min: "-inf", Max: "0"}).Val()
}

func (c *Client) IndexJobYears(jobId string, skillYears map[string]int64) {
	totalSkills := utils.Set{}
	pipeline := c.Conn.TxPipeline()
	for skill, years := range skillYears {
		totalSkills.Add(skill)
		pipeline.ZAdd(fmt.Sprintf("idx:skill:%s:years", skill),
			&redis.Z{Member: jobId, Score: float64(utils.Max(years, 0))})
	}
	pipeline.SAdd("idx:jobs:all", jobId)
	pipeline.ZAdd("idx:jobs:req", &redis.Z{Member: jobId, Score: float64(len(totalSkills))})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in IndexJobLevels: ", err)
		return
	}
}

func (c *Client) SearchJobYears(skillyears map[string]int64) []string {
	pipeline := c.Conn.TxPipeline()
	union := []string{}
	for skill, years := range skillyears {
		subResult := c.Zintersect(map[string]float64{"jobs:all": float64(-years),
			fmt.Sprintf("skill:%s:years", skill): 1}, 30, "")
		c.Conn.ZRemRangeByScore("idx:"+subResult, "(0", "inf")
		union = append(union, c.Zintersect(map[string]float64{"jobs:all": 1, subResult: 0}, 30, ""))
	}

	unionmap := make(map[string]float64, len(union))
	for _, v := range union {
		unionmap[v] = 1
	}

	jobScores := c.ZUnion(unionmap, 30, "")
	finalResult := c.Zintersect(map[string]float64{jobScores: -1, "jobs:req": 1}, 30, "")
	var res *redis.StringSliceCmd
	res = pipeline.ZRangeByScore("idx:"+finalResult, &redis.ZRangeBy{Min: "-inf", Max: "0"})
	_, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in IndexJobLevels: ", err)
		return nil
	}
	return res.Val()
}
