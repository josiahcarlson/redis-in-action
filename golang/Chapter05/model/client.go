package model

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
	"log"
	"math"
	"redisInAction/Chapter05/common"
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

func (c *Client) LogRecent(name, message, severity string, pipeliner redis.Pipeliner) {
	if severity == "" {
		severity = "INFO"
	}
	destination := fmt.Sprintf("recent:%s:%s", name, severity)
	message = time.Now().Local().String() + " " + message

	if pipeliner == nil {
		pipeliner = c.Conn.Pipeline()
	}
	pipeliner.LPush(destination, message)
	pipeliner.LTrim(destination, 0, 99)
	if _, err := pipeliner.Exec(); err != nil {
		log.Println("LogRecent pipline err: ", err)
	}
}

func (c *Client) LogCommon(name, message, severity string, timeout int64) {
	destination := fmt.Sprintf("common:%s:%s", name, severity)
	startKey := destination + ":start"
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)

	for time.Now().Before(end) {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			hourStart := time.Now().Local().Hour()
			existing, _ := strconv.Atoi(tx.Get(startKey).Val())

			if _, err := tx.Pipelined(func(pipeliner redis.Pipeliner) error {
				if existing != 0 && existing < hourStart {
					pipeliner.Rename(destination, destination+":last")
					pipeliner.Rename(startKey, destination+":pstart")
					pipeliner.Set(startKey, hourStart, 0)
				} else if existing == 0 {
					pipeliner.Set(startKey, hourStart, 0)
				}

				pipeliner.ZIncrBy(destination, 1, message)
				c.LogRecent(name, message, severity, pipeliner)
				return nil
			}); err != nil {
				log.Println("LogCommon pipelined failed, err: ", err)
				return err
			}
			return nil
		}, startKey)
		if err != nil {
			log.Println("watch failed, err: ", err)
			continue
		}
	}
}

func (c *Client) UpdateCounter(name string, count int64, now int64) {
	if now == 0 {
		now = time.Now().Unix()
	}

	pipe := c.Conn.Pipeline()
	for _, prec := range common.PRECISION {
		pnow := (now / prec) * prec
		hash := fmt.Sprintf("%d:%s", prec, name)
		pipe.ZAdd("known:", &redis.Z{Member: hash, Score: 0})
		pipe.HIncrBy("count:"+hash, strconv.Itoa(int(pnow)), count)
	}
	if _, err := pipe.Exec(); err != nil {
		log.Println("updateCounter err: ", err)
	}
}

func (c *Client) GetCount(name, precision string) [][]int {
	hash := fmt.Sprintf("%v:%s", precision, name)
	data := c.Conn.HGetAll("count:" + hash).Val()
	toReturn := make([][]int, 0, len(data))
	for k, v := range data {
		temp := make([]int, 2)
		key, _ := strconv.Atoi(k)
		num, _ := strconv.Atoi(v)
		temp[0], temp[1] = key, num
		toReturn = append(toReturn, temp)
	}
	sort.Slice(toReturn, func(i, j int) bool {
		return toReturn[i][0] < toReturn[j][0]
	})
	return toReturn
}

func (c *Client) CleanCounters() {
	passes := 0

	for !common.QUIT {
		start := time.Now().Unix()
		var index int64 = 0
		for index < c.Conn.ZCard("known:").Val() {
			hash := c.Conn.ZRange("known:", index, index).Val()
			index++
			if len(hash) == 0 {
				break
			}

			hashValue := hash[0]
			prec, _ := strconv.Atoi(strings.Split(hashValue, ":")[0])
			bprec := prec / 60
			if bprec == 0 {
				bprec = 1
			}
			if passes%bprec != 0 {
				continue
			}

			hkey := "count:" + hashValue
			cutoff := int(time.Now().Unix()) - common.SAMPLECOUNT*prec
			samples := c.Conn.HKeys(hkey).Val()
			sort.Slice(samples, func(i, j int) bool {
				return samples[i] < samples[j]
			})
			remove := sort.SearchStrings(samples, strconv.Itoa(cutoff))

			if remove != 0 {
				c.Conn.HDel(hkey, samples[:remove]...)
				if remove == len(samples) {
					err := c.Conn.Watch(func(tx *redis.Tx) error {
						if tx.HLen(hkey).Val() == 0 {
							pipe := tx.Pipeline()
							pipe.ZRem("known:", hashValue)
							_, err := pipe.Exec()
							if err != nil {
								log.Println("pipeline failed in CleanCounters: ", err)
								return err
							}
							index--
						} else {
							tx.Unwatch()
						}
						return nil
					}, hkey)

					if err != nil {
						log.Println("watch err in CleanCounters: ", err)
						continue
					}
				}
			}
		}
		passes++
		duration := utils.Min(time.Now().Unix()-start+1, 60)
		time.Sleep(time.Duration(utils.Max(60-duration, 1)) * time.Minute)
	}
}

func (c *Client) UpdateStats(context, types string, value float64, timeout int64) []redis.Cmder {
	destination := fmt.Sprintf("stats:%s:%s", context, types)
	startKey := destination + ":start"
	end := time.Now().Add(time.Duration(timeout) * time.Millisecond)

	var res []redis.Cmder
	for time.Now().Before(end) {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			hourStart := time.Now().Local().Hour()
			existing, _ := strconv.Atoi(tx.Get(startKey).Val())

			if _, err := tx.Pipelined(func(pipeliner redis.Pipeliner) error {
				if existing == 0 {
					pipeliner.Set(startKey, hourStart, 0)
				} else if existing < hourStart {
					pipeliner.Rename(destination, destination+":last")
					pipeliner.Rename(startKey, destination+":pstart")
					pipeliner.Set(startKey, hourStart, 0)
				}

				tkey1 := uuid.NewV4().String()
				tkey2 := uuid.NewV4().String()
				pipeliner.ZAdd(tkey1, &redis.Z{Member: "min", Score: value})
				pipeliner.ZAdd(tkey2, &redis.Z{Member: "max", Score: value})
				pipeliner.ZUnionStore(destination, &redis.ZStore{Aggregate: "MIN", Keys: []string{destination, tkey1}})
				pipeliner.ZUnionStore(destination, &redis.ZStore{Aggregate: "MAX", Keys: []string{destination, tkey2}})

				pipeliner.Del(tkey1, tkey2)
				pipeliner.ZIncrBy(destination, 1, "count")
				pipeliner.ZIncrBy(destination, value, "sum")
				pipeliner.ZIncrBy(destination, value*value, "sumq")
				res, _ = pipeliner.Exec()
				res = res[len(res)-3:]
				return nil
			}); err != nil {
				log.Println("pipeline filed in UpdateStats: ", err)
				return err
			}
			return nil
		}, startKey)

		if err != nil {
			log.Println("watch filed in UpdateStats: ", err)
			continue
		}
	}
	return res
}

func (c *Client) GetStats(context, types string) map[string]float64 {
	key := fmt.Sprintf("stats:%s:%s", context, types)
	stats := map[string]float64{}
	data := c.Conn.ZRangeWithScores(key, 0, -1).Val()
	for _, v := range data {
		stats[v.Member.(string)] = v.Score
	}
	stats["average"] = stats["sum"] / stats["count"]
	numerator := stats["sumq"] - (math.Pow(stats["sum"], 2) / stats["count"])
	count := stats["count"]
	if count > 1 {
		count--
	} else {
		count = 1
	}
	stats["stddev"] = math.Pow(numerator/count, 0.5)
	return stats
}

func (c *Client) AccessTime(context string, f func()) {
	start := time.Now().Unix()
	f()
	delta := time.Now().Unix() - start
	stats := c.UpdateStats(context, "AccessTime", float64(delta), 5)
	average := stats[1].(*redis.FloatCmd).Val() / stats[0].(*redis.FloatCmd).Val()

	pipe := c.Conn.TxPipeline()
	pipe.ZAdd("slowest:AccessTime", &redis.Z{Member:context, Score: average})
	pipe.ZRemRangeByRank("slowest:AccessTime", 0, -101)
	if _, err := pipe.Exec(); err != nil {
		log.Println("pipeline err in AccessTime: ", err)
	}
}

func (c *Client) IpToScore(ip string) int64 {
	var score int64 = 0
	for _, v := range strings.Split(ip, ".") {
		n, _ := strconv.ParseInt(v, 10, 0)
		score = score*256 + n
	}
	return score
}

func (c *Client) ImportIpsToRedis(filename string) {
	res := utils.CSVReader(filename)
	pipe := c.Conn.Pipeline()
	for count, row := range res {
		var (
			startIp string
			resIP   int64
		)
		if len(row) == 0 {
			startIp = ""
		} else {
			startIp = row[0]
		}
		if strings.Contains(strings.ToLower(startIp), "i") {
			continue
		}
		if strings.Contains(startIp, ".") {
			resIP = c.IpToScore(startIp)
		} else {
			var err error
			resIP, err = strconv.ParseInt(startIp, 10, 64)
			if err != nil {
				continue
			}
		}
		cityID := row[2] + "_" + strconv.Itoa(count)
		pipe.ZAdd("ip2cityid:", &redis.Z{Member: cityID, Score: float64(resIP)})
		if (count+1)%1000 == 0 {
			if _, err := pipe.Exec(); err != nil {
				log.Println("pipeline err in ImportIpsToRedis: ", err)
				return
			}
		}
	}

	if _, err := pipe.Exec(); err != nil {
		log.Println("pipeline err in ImportIpsToRedis: ", err)
		return
	}
}

type cityInfo struct {
	CityId  string
	Country string
	Region  string
	City    string
}

func (c *Client) ImportCityToRedis(filename string) {
	res := utils.CSVReader(filename)
	pipe := c.Conn.Pipeline()
	for count, row := range res {
		if len(row) < 4 || !utils.IsDigital(row[0]) {
			continue
		}

		city := cityInfo{
			CityId:  row[0],
			Country: row[1],
			Region:  row[2],
			City:    row[3],
		}

		value, err := json.Marshal(city)
		if err != nil {
			log.Println("marshal json failed, err: ", err)
		}
		pipe.HSet("cityid2city:", city.CityId, value)
		if (count+1)%1000 == 0 {
			if _, err := pipe.Exec(); err != nil {
				log.Println("pipeline err in ImportCityToRedis: ", err)
				return
			}
		}
	}

	if _, err := pipe.Exec(); err != nil {
		log.Println("pipeline err in ImportCityToRedis: ", err)
		return
	}
}

func (c *Client) FindCityByIp(ip string) string {
	ipAddress := strconv.Itoa(int(c.IpToScore(ip)))
	res := c.Conn.ZRangeByScore("ip2cityid:", &redis.ZRangeBy{Max: ipAddress, Min: "0", Offset: 0, Count: 2}).Val()
	if len(res) == 0 {
		return ""
	}
	cityId := strings.Split(res[0], "_")[0]
	var result cityInfo
	if err := json.Unmarshal([]byte(c.Conn.HGet("cityid2city:", cityId).Val()), &result); err != nil {
		log.Fatalln("unmarshal err: ", err)
	}
	return strings.Join([]string{result.CityId, result.City, result.Country, result.Region}, " ")
}

func (c *Client) IsUnderMaintenance() bool {
	if common.LASTCHECKED < time.Now().Unix()-1 {
		common.LASTCHECKED = time.Now().Unix()
		common.ISUNDERMAINTENANCE = c.Conn.Get("is-under-maintenance").Val() == "yes"
	}
	return common.ISUNDERMAINTENANCE
}

func (c *Client) SetConfig(types, component string, config map[string]interface{}) {
	val, err := json.Marshal(config)
	if err != nil {
		log.Fatalln("marshal in SetConfig err: ", err)
	}
	c.Conn.Set(fmt.Sprintf("config:%s:%s", types, component), val, 0)
}

func (c *Client) GetConfig(types, comonent string, wait int64) map[string]interface{} {
	key := fmt.Sprintf("config:%s:%s", types, comonent)
	ch, ok := common.CHECKED[key]
	if !ok || ch < time.Now().Unix()-wait {
		common.CHECKED[key] = time.Now().Unix()
		config := map[string]interface{}{}
		if err := json.Unmarshal([]byte(c.Conn.Get(key).Val()), &config); err != nil {
			config = map[string]interface{}{}
			return nil
		}
		oldConfig := common.CONFIG[key]
		if !reflect.DeepEqual(oldConfig, config) {
			common.CONFIG[key] = config
		}
	}
	return common.CONFIG[key]
}

var checked = map[string]int64{}
var configs = map[string]map[string]string{}
var redisConnections = map[string]map[string]string{}

func (c *Client) SetConfigs(types, component string, config map[string]string) {
	res, err := json.Marshal(config)
	if err != nil {
		log.Println("marshal json err: ", err)
		return
	}
	c.Conn.Set(fmt.Sprintf("config:%s:%s", types, component), res, 0)
}

func (c *Client) GetConfigs(types, component string, wait int64) map[string]string {
	key := fmt.Sprintf("config:%s:%s", types, component)

	if ch, ok := checked[key]; !ok || ch < time.Now().Unix() - wait {
		checked[key] = time.Now().Unix()
		config := map[string]string{}
		if err := json.Unmarshal([]byte(c.Conn.Get(key).Val()), &config); err != nil {
			log.Println("unmarshal err in GetConfigs: ", err)
		}
		oldConfig := configs[key]

		if !reflect.DeepEqual(oldConfig, config) {
			configs[key] = config
		}
	}
	return configs[key]
}

func (c *Client) RedisConnenction(component string, wait int64) func() map[string]string {
	key := "config:redis:" + component
	return func() map[string]string {
		oldConfig := configs[key]
		config := c.GetConfigs("redis", component, wait)

		if !reflect.DeepEqual(config, oldConfig) {
			redisConnections[key] = config
		}
		return redisConnections[key]
	}
}