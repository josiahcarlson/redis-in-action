package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
	"log"
	"redisInAction/Chapter07/common"
	"redisInAction/utils"
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

func Tokenize(content string) []string {
	words := utils.Set{}
	for _, match := range common.WORDSRE.FindAllString(strings.ToLower(content), -1) {
		if len(match) < 2 {
			continue
		}
		words.Add(match)
	}
	return words.Intersection(&common.STOPWORDS)
}

func (c *Client) IndexDocument(docid, content string) int64 {
	words := Tokenize(content)

	pipeline := c.Conn.TxPipeline()
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

func (c *Client) setCommon(method string, names *[]string, ttl int) string {
	id := uuid.NewV4().String()
	pipeline := c.Conn.TxPipeline()

	namelist := make([]reflect.Value, 0, len(*names)+1)
	namelist = append(namelist, reflect.ValueOf("idx:"+id))
	for _, name := range *names {
		namelist = append(namelist, reflect.ValueOf("idx:"+name))
	}
	methodValue := reflect.ValueOf(pipeline).MethodByName(method)
	methodValue.Call(namelist)
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

func Parse(query string) (all [][]string, unwantedlist []string) {
	unwanted, current := utils.Set{}, utils.Set{}
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
			unwanted.Add(word)
			continue
		case len(current) != 0 && prefix == 0:
			all = append(all, current.Getkeys())
			current = utils.Set{}
		}

		current.Add(word)
	}

	if len(current) != 0 {
		all = append(all, current.Getkeys())
	}

	unwantedlist = append(unwantedlist, unwanted.Getkeys()...)
	return
}

func (c *Client) ParseAndSearch(query string, ttl int) string {
	all, unwanted := Parse(query)

	if len(all) == 0 {
		return ""
	}

	toIntersect := []string{}
	for _, syn := range all {
		if len(syn) > 1 {
			toIntersect = append(toIntersect, c.Union(syn, ttl))
		} else {
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

func (c *Client) SearchAndSort(query string, id string, ttl int, sort string, start, num int64) ([]string, string) {
	var order string
	if strings.HasPrefix(sort, "-") {
		order = "DESC"
	} else {
		order = "ASC"
	}

	sort = strings.TrimPrefix(sort, "-")
	by := "kb:doc:*->" + sort

	alpha := !strings.Contains(sort, "updated") && !strings.Contains(sort, "id") &&
		!strings.Contains(sort, "created")

	if id != "" && !c.Conn.Expire(id, time.Duration(ttl)*time.Second).Val() {
		id = ""
	}

	if id == "" {
		id = c.ParseAndSearch(query, ttl)
	}

	var res *redis.StringSliceCmd
	pipeline := c.Conn.TxPipeline()
	pipeline.SCard("idx:" + id)
	res = pipeline.Sort("idx:"+id, &redis.Sort{By: by, Alpha: alpha, Order: order, Offset: start, Count: num})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in SearchAndSort: ", err)
		return nil, ""
	}
	return res.Val(), id
}

func (c *Client) SearchAndZsort(query string, id string, ttl int, update, vote float64, start, num int64,
	desc bool) ([]string, string) {
	if id != "" && !c.Conn.Expire(id, time.Duration(ttl)*time.Second).Val() {
		id = ""
	}

	if id == "" {
		id = c.ParseAndSearch(query, ttl)

		scoredSearch := map[string]float64{
			id:            0,
			"sort:update": update,
			"sort:votes":  vote,
		}
		id = c.Zintersect(scoredSearch, 300, "")
	}

	pipeline := c.Conn.TxPipeline()
	pipeline.ZCard("idx:" + id)
	var res *redis.StringSliceCmd
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
	pipeline.ZAdd("idx:ad:value:", &redis.Z{Member: id, Score: rvalue})
	pipeline.ZAdd("ad:baseValue:", &redis.Z{Member: id, Score: value})
	//pipeline.SAdd("terms:"+id, words) FLAG
	for _, word := range words {
		pipeline.SAdd("terms:" + id, word)
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

func (c *Client) TargetAds(locations []string, content string) (string, string) {
	matchedAds, baseEcpm := c.matchLocation(&locations)
	words, targetedAds := c.finishScoring(matchedAds, baseEcpm, content)

	pipeline := c.Conn.TxPipeline()
	tempAd := &redis.StringSliceCmd{}
	targetId := &redis.IntCmd{}
	targetId = pipeline.Incr("ads:served:")
	tempAd = pipeline.ZRevRange("idx:"+targetedAds, 0, 0)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in TargetAds: ", err)
	}
	targetedAd := tempAd.Val()
	if len(targetedAd) == 0 {
		return "", ""
	}

	adId := targetedAd[0]
	c.recordTargetingResult(strconv.Itoa(int(targetId.Val())), adId, words)
	return strconv.Itoa(int(targetId.Val())), adId
}

func (c *Client) matchLocation(locations *[]string) (string, string) {
	var required []string
	for _, loc := range *locations {
		required = append(required, "req:"+loc)
	}
	matchedAds := c.Union(required, 300)
	baseEcpm := c.Zintersect(map[string]float64{matchedAds: 0, "ad:value:": 1}, 30, "")

	return matchedAds, baseEcpm
}

func (c *Client) finishScoring(matched, base, content string) ([]string, string) {
	bonusEcpm := map[string]float64{}
	words := Tokenize(content)
	for _, word := range words {
		wordBonus := c.Zintersect(map[string]float64{matched: 0, word: 1}, 30, "")
		bonusEcpm[wordBonus] = 1
	}

	if len(bonusEcpm) != 0 {
		minimum := c.ZUnion(bonusEcpm, 30, "MIN")
		maximum := c.ZUnion(bonusEcpm, 30, "MAX")
		return words, c.ZUnion(map[string]float64{base: 1, minimum: 0.5, maximum: 0.5}, 30, "")
	}
	return words, base
}

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

	if len(matched) != 0 {
		matchedKey := fmt.Sprintf("terms:matched:%s", targetId)
		for _, word := range matched {
			pipeline.SAdd(matchedKey, word)
		}
		pipeline.Expire(matchedKey, 900*time.Second)
	}

	types := c.Conn.HGet("type:", adId).Val()
	pipeline.Incr(fmt.Sprintf("type:%s:views:", types))
	for _, word := range matched {
		pipeline.ZIncrBy(fmt.Sprintf("views:%s", adId), 1, word)
	}
	pipeline.ZIncrBy(fmt.Sprintf("views:%s", adId), 1, "")

	res, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in recordTargetingResult : ", err)
		return
	}

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
	if err != nil && err != redis.Nil{
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
	common.AVERAGEPER1K[types] = float64(1000 * typeClicks) / float64(typeViews)

	if types == "cpm" {
		return
	}

	viewKey := fmt.Sprintf("views:%s", adId)
	clickKey := fmt.Sprintf("%s:%s", which, adId)

	pipeline.ZScore(viewKey, "")
	pipeline.ZScore(clickKey, "")
	res, err = pipeline.Exec()
	if err != nil && err != redis.Nil{
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
		if err != nil && err != redis.Nil{
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

func (c *Client) RecordClick(targetId, adId string, action bool) {
	pipeline := c.Conn.TxPipeline()
	clickKey := fmt.Sprintf("clicks:%s", adId)

	matchKey := fmt.Sprintf("terms:matched:%s", targetId)

	types := c.Conn.HGet("type:", adId).Val()
	if types == "cpa" {
		pipeline.Expire(matchKey, 900*time.Second)
		if action {
			clickKey = fmt.Sprintf("actions:%s", adId)
		}
	}

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
