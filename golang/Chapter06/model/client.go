package model

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	uuid "github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"redisInAction/Chapter06/common"
	"redisInAction/utils"
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

func (c *Client) AddUpdateContact(user, contact string) {
	acList := "recent:" + user
	pipeline := c.Conn.TxPipeline()
	pipeline.LRem(acList, 1, contact)
	pipeline.LPush(acList, contact)
	pipeline.LTrim(acList, 0, 99)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in AddUpdateContact: ", err)
	}
}

func (c *Client) RemoveContact(user, contact string) {
	c.Conn.LRem("recent:"+user, 1, contact)
}

func (c *Client) FetchAutoCompleteList(user, prefix string) []string {
	candidates := c.Conn.LRange("recent:"+user, 0, -1).Val()
	var matches []string
	for _, candidate := range candidates {
		if strings.HasPrefix(strings.ToLower(candidate), strings.ToLower(prefix)) {
			matches = append(matches, candidate)
		}
	}
	return matches
}

func (c *Client) FindPrefixRange(prefix string) (string, string) {
	posn := strings.IndexByte(common.ValidCharacters, prefix[len(prefix)-1])
	if posn == 0 {
		posn = 1
	}
	suffix := string(common.ValidCharacters[posn-1])
	return prefix[:len(prefix)-1] + suffix + "{", prefix + "{"
}

func (c *Client) AutoCompleteOnPrefix(guild, prefix string) []string {
	start, end := c.FindPrefixRange(prefix)
	identifier := uuid.NewV4().String()
	start += identifier
	end += identifier
	zsetName := "members:" + guild

	var items []string
	c.Conn.ZAdd(zsetName, &redis.Z{Member: start, Score: 0}, &redis.Z{Member: end, Score: 0})
	for {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			pipeline := tx.TxPipeline()
			sindex := tx.ZRank(zsetName, start).Val()
			eindex := tx.ZRank(zsetName, end).Val()
			erange := utils.Min(sindex+9, eindex-2)
			pipeline.ZRem(zsetName, start, end)
			var tmp *redis.StringSliceCmd
			tmp = pipeline.ZRange(zsetName, sindex, erange)
			_, err := pipeline.Exec()
			if err != nil {
				log.Println("pipeline err in AutoCompleteOnPrefix: ", err)
				return err
			}
			res := tmp.Val()
			if len(res) != 0 {
				items = res
			}
			return nil
		}, zsetName)
		if err != nil {
			continue
		}
		break
	}
	var result []string
	for _, item := range items {
		if !strings.Contains(item, "{") {
			result = append(result, item)
		}
	}
	return result
}

func (c *Client) JoinGuild(guild, user string) {
	c.Conn.ZAdd("members:"+guild, &redis.Z{Member: user, Score: 0})
}

func (c *Client) LeaveGuild(guild, user string) {
	c.Conn.ZRem("members:"+guild, user)
}

func (c *Client) AcquireLock(lockname string, acquireTimeout float64) string {
	identifier := uuid.NewV4().String()

	end := time.Now().UnixNano() + int64(acquireTimeout*1e6)
	for time.Now().UnixNano() < end {
		if c.Conn.SetNX("lock:"+lockname, identifier, 0).Val() {
			return identifier
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ""
}

func (c *Client) PurchaseItemWithLock(buyerId, itemId, sellerId string) bool {
	buyer := fmt.Sprintf("users:%s", buyerId)
	seller := fmt.Sprintf("users:%s", sellerId)
	item := fmt.Sprintf("%s.%s", itemId, sellerId)
	inventory := fmt.Sprintf("inventory:%s", buyerId)

	locked := c.AcquireLock("market:", 10)
	defer c.ReleaseLock("market:", locked)
	if locked == "" {
		return false
	}

	var (
		price float64
		funds float64
	)

	resZscore := &redis.FloatCmd{}
	resHget := &redis.StringCmd{}

	pipe := c.Conn.TxPipeline()
	if err := c.Conn.Watch(func(tx *redis.Tx) error {
		resZscore = pipe.ZScore("market:", item)
		resHget = tx.HGet(buyer, "funds")
		if _, err := pipe.Exec(); err != nil {
			log.Println("pipeline err in watch func of PurchaseItemWithLock: ", err)
			return err
		}
		price = resZscore.Val()
		funds, _ = strconv.ParseFloat(resHget.Val(), 64)
		return nil
	}); err != nil {
		log.Println("tx err in PurchaseItemWithLock: ", err)
		return false
	}


	if price == 0 || price > funds {
		return false
	}

	pipe.HIncrBy(seller, "funds", int64(price))
	pipe.HIncrBy(buyer, "funds", int64(-price))
	pipe.SAdd(inventory, itemId)
	pipe.ZRem("market:", item)
	if _, err := pipe.Exec(); err != nil {
		log.Println("pipeline failed in PurchaseItemWithLock: ", err)
		return false
	}
	return true
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

func (c *Client) AcquireSemaphore(semname string, limit int64, timeout int64) string {
	identifier := uuid.NewV4().String()
	now := time.Now().Unix()

	var res *redis.IntCmd
	pipeline := c.Conn.TxPipeline()
	pipeline.ZRemRangeByScore(semname, "-inf", strconv.Itoa(int(now-timeout)))
	pipeline.ZAdd(semname, &redis.Z{Member: identifier, Score: float64(now)})
	res = pipeline.ZRank(semname, identifier)
	_, err := pipeline.Exec()
	if err != nil {
		log.Println("pipeline err in AcquireSemaphore: ", err)
	}
	if res.Val() < limit {
		return identifier
	}

	c.Conn.ZRem(semname, identifier)
	return ""
}

func (c *Client) ReleaseSemaphore(semname, identifier string) {
	c.Conn.ZRem(semname, identifier)
}

func (c *Client) AcquireFairSemaphore(semname string, limit, timeout int64) string {
	identifier := uuid.NewV4().String()
	czset := semname + ":owner"
	ctr := semname + ":counter"

	now := time.Now().Unix()
	pipeline := c.Conn.TxPipeline()
	pipeline.ZRemRangeByScore(semname, "-inf", strconv.Itoa(int(now-timeout)))
	pipeline.ZInterStore(czset, &redis.ZStore{Keys: []string{czset, semname}, Weights: []float64{1, 0}})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in AcquireFairSemaphore: ", err)
		return ""
	}
	counter := c.Conn.Incr(ctr).Val()

	pipeline.ZAdd(semname, &redis.Z{Member: identifier, Score: float64(now)})
	pipeline.ZAdd(czset, &redis.Z{Member: identifier, Score: float64(counter)})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in AcquireFairSemaphore: ", err)
		return ""
	}

	res := c.Conn.ZRank(czset, identifier).Val()
	if res < limit {
		return identifier
	}

	pipeline.ZRem(semname, identifier)
	pipeline.ZRem(czset, identifier)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in AcquireFairSemaphore: ", err)
		return ""
	}
	return ""
}

func (c *Client) ReleaseFairSemaphore(semname, identifier string) bool {
	pipeline := c.Conn.TxPipeline()
	pipeline.ZRem(semname, identifier)
	pipeline.ZRem(semname+":owner", identifier)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in ReleaseFairSemaphore: ", err)
		return false
	}
	return true
}

func (c *Client) RefreshFairSemaphore(semname, identifier string) bool {
	if c.Conn.ZAdd(semname, &redis.Z{Member: identifier, Score: float64(time.Now().Unix())}).Val() != 0 {
		c.ReleaseFairSemaphore(semname, identifier)
		return false
	}
	return true
}

func (c *Client) AcquireSemaphoreWithLock(semname string, limit int64, timeout int64) string {
	identifier := c.AcquireLock(semname, 0.01)
	if identifier != "" {
		return c.AcquireFairSemaphore(semname, limit, timeout)
	}
	defer c.ReleaseLock(semname, identifier)
	return ""
}

type soldData struct {
	SellerId string
	ItemId   string
	Price    string
	BuyerId  string
	Time     int64
}

func (c *Client) SendSoldEmailViaQueue(seller, item, price, buyer string) {
	data := soldData{
		SellerId: seller,
		ItemId:   item,
		Price:    price,
		BuyerId:  buyer,
		Time:     time.Now().Unix(),
	}
	jsonValue, err := json.Marshal(data)
	if err != nil {
		log.Println("marshal err in SendSoldEmailViaQueue: ", err)
		return
	}
	c.Conn.RPush("queue:email", jsonValue)
}

func (c *Client) ProcessSoldEmailQueue() {
	for !common.QUIT {
		packed := c.Conn.BLPop(30*time.Second, "queue:email").Val()
		if len(packed) == 0 {
			continue
		}

		toSend := soldData{}
		if err := json.Unmarshal([]byte(packed[0]), &toSend); err != nil {
			log.Println("unmarshal err in ProcessSoldEmailQueue: ", err)
			return
		}

		SendEmail()
	}
}

func SendEmail() {}

type info struct {
	Identifier string
	Queue      string
	Name       string
	Args       []string
}

func (c *Client) ExecuteLater(queue, name string, args []string, delay float64) string {
	identifier := uuid.NewV4().String()
	data := info{
		Identifier: identifier,
		Queue:      queue,
		Name:       name,
		Args:       args,
	}

	item, err := json.Marshal(data)
	if err != nil {
		log.Println("marshal err in ExecuteLater: ", err)
		return ""
	}

	if delay > 0 {
		c.Conn.ZAdd("delayed:", &redis.Z{Member: item, Score: float64(time.Now().UnixNano() + int64(delay*1e9))})
	} else {
		c.Conn.RPush("queue:"+queue, item)
	}
	return identifier
}

func (c *Client) PollQueue(channel chan struct{}) {
	for !common.QUIT {
		item := c.Conn.ZRangeWithScores("delayed:", 0, 0).Val()
		if len(item) == 0 || int64(item[0].Score) > time.Now().UnixNano() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		res := item[0].Member.(string)
		data := info{}
		if err := json.Unmarshal([]byte(res), &data); err != nil {
			log.Println("unmarshal err in PollQueue: ", err)
			channel <- struct{}{}
			return
		}

		locked := c.AcquireLock(data.Identifier, 10)
		if locked == "" {
			continue
		}

		if c.Conn.ZRem("delayed:", res).Val() != 0 {
			c.Conn.RPush("queue:"+data.Queue, res)
		}

		c.ReleaseLock(data.Identifier, locked)
	}

	channel <- struct{}{}
	defer close(channel)
}

func (c *Client) CreateChat(sender string, recipients *[]string, message string, chatId string) string {
	if chatId == "" {
		chatId = strconv.Itoa(int(c.Conn.Incr("ids:chat:").Val()))
	}

	*recipients = append(*recipients, sender)
	var recipientsd []*redis.Z
	for _, r := range *recipients {
		temp := redis.Z{
			Score:  0,
			Member: r,
		}
		recipientsd = append(recipientsd, &temp)
	}

	pipeline := c.Conn.TxPipeline()
	pipeline.ZAdd("chat:"+chatId, recipientsd...)
	for _, rec := range *recipients {
		pipeline.ZAdd("seen:"+rec, &redis.Z{Member: chatId, Score: 0})
	}
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in CreateChat: ", err)
	}
	return c.SendMessage(chatId, sender, message)
}

type Pack struct {
	Id      int64
	Ts      int64
	Sender  string
	Message string
}

type Messages struct {
	ChatId string
	ChatMessages []Pack
}

func (c *Client) SendMessage(chatId, sender, message string) string {
	identifier := c.AcquireLock("chat:"+chatId, 10)
	if identifier == "" {
		log.Println("Couldn't get the lock")
		return ""
	}

	mid := c.Conn.Incr("ids:" + chatId).Val()
	ts := time.Now().Unix()
	packed := Pack{
		Id:      mid,
		Ts:      ts,
		Sender:  sender,
		Message: message,
	}

	jsonValue, err := json.Marshal(packed)
	if err != nil {
		log.Println("marshal err in SendMessage: ", err)
	}

	c.Conn.ZAdd("msgs:"+chatId, &redis.Z{Member: jsonValue, Score: float64(mid)})
	defer c.ReleaseLock("chat:"+chatId, identifier)
	return chatId
}

func (c *Client) FetchPendingMessage(recipient string) []Messages {
	seen := c.Conn.ZRangeWithScores("seen:" + recipient, 0, -1).Val()
	pipeline := c.Conn.TxPipeline()

	res := &redis.StringSliceCmd{}
	length := len(seen)
	temp := make([]string, 0, length)
	for _, v := range seen {
		chatId := v.Member.(string)
		seenId := v.Score
		res = pipeline.ZRangeByScore("msgs:" + chatId, &redis.ZRangeBy{Min:strconv.Itoa(int(seenId + 1)), Max:"inf"})
		temp = append(temp, chatId, strconv.Itoa(int(seenId)))
	}

	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in FetchPendingMessage: ", err)
		return nil
	}
	chatInfo := [][]string{temp, res.Val()}
	result := make([]Messages, len(chatInfo) / 2)

	for i := 0; i < len(chatInfo); i += 2 {
		if len(chatInfo[i + 1]) == 0 {
			continue
		}

		messages := []Pack{}
		for _, v := range chatInfo[i+1] {
			message :=Pack{}
			if err := json.Unmarshal([]byte(v), &message); err != nil {
				log.Println("unmarshal err in FetchPendingMessage: ", err)
			}
			messages = append(messages, message)
		}

		chatId := chatInfo[i][0]
		seenId := float64(messages[len(messages) - 1].Id)
		c.Conn.ZAdd("chat:" + chatId, &redis.Z{Member:recipient, Score: seenId})

		minId := c.Conn.ZRangeWithScores("chat:" + chatId, 0, 0).Val()

		pipeline.ZAdd("seen:" + recipient, &redis.Z{Member:chatId, Score:seenId})
		if minId != nil {
			pipeline.ZRemRangeByScore("msgs:" + chatId, string(0), strconv.Itoa(int(minId[0].Score)))
		}
		result[i] = Messages{ChatId:chatId, ChatMessages:messages}
	}

	return result
}

func (c *Client) JoinChat(chatId, user string) {
	messageId, _ := c.Conn.Get("ids" + chatId).Float64()

	pipeline := c.Conn.TxPipeline()
	pipeline.ZAdd("chat:" + chatId, &redis.Z{Member:user, Score:messageId})
	pipeline.ZAdd("seen:" + user, &redis.Z{Member:chatId, Score:messageId})
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in JoinChat: ", err)
	}
}

func (c *Client) LeaveChat(chatId, user string) {
	res := &redis.IntCmd{}
	pipeline := c.Conn.TxPipeline()
	pipeline.ZRem("chat:" + chatId, user)
	pipeline.ZRem("seen:" + user, chatId)
	res = pipeline.ZCard("chat:" + chatId)
	if _, err := pipeline.Exec(); err != nil {
		log.Println("pipeline err in LeaveChat: ", err)
	}

	if res == nil {
		pipeline.Del("msgs:" + chatId)
		pipeline.Del("ids:" + chatId)
		if _, err := pipeline.Exec(); err != nil {
			log.Println("pipeline err in LeaveChat: ", err)
		}
	} else {
		oldest := c.Conn.ZRangeWithScores("chat:" + chatId, 0, 0).Val()[0]
		c.Conn.ZRemRangeByScore("msgs:" + chatId, "0", strconv.Itoa(int(oldest.Score)))
	}
}

func (c *Client) CopyLogsToRedis(path, channel string, count int, limit int64, quitWhenDone bool) {
	var bytesInRedis int64 = 0
	waiting := []os.FileInfo{}
	recipies := make([]string, count)
	for i := 0; i < count; i++ {
		recipies[i] = strconv.Itoa(i)
	}
	c.CreateChat("source", &recipies, "", channel)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Println("get dir err in CopyLogsToRedis: ", err)
		return
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})
	for _, logfile := range files {
		fullPath := filepath.Join(path, logfile.Name())

		finfo, _ := os.Stat(fullPath)
		fsize := finfo.Size()
		for bytesInRedis + fsize > limit {
			cleaned := c.clean(channel, &waiting, count)
			if cleaned != 0 {
				bytesInRedis -= cleaned
			} else {
				time.Sleep(250 * time.Millisecond)
			}
		}

		file, err := os.Open(fullPath)
		if err != nil {
			log.Fatalln("open file in CopyLogsToRedis err: ", err)
			return
		}

		byteSlice := make([]byte, int(math.Pow(2, 17)))
		for {
			temp, _ := file.Read(byteSlice)
			byteSlice = byteSlice[:temp]
			if temp == 0 {
				break
			}
			c.Conn.Append(channel + logfile.Name(), string(byteSlice))
		}
		c.SendMessage(channel, "source", logfile.Name())

		bytesInRedis += fsize
		waiting = append(waiting, logfile)
	}

	if quitWhenDone {
		c.SendMessage(channel, "source", ":done")
	}

	for len(waiting) != 0 {
		cleaned := c.clean(channel, &waiting, count)
		if cleaned != 0 {
			bytesInRedis -= cleaned
		} else {
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func (c *Client) clean(channel string, waiting *[]os.FileInfo, count int) int64 {
	if len(*waiting) == 0 {
		return 0
	}

	w0 := (*waiting)[0].Name()
	res, err := c.Conn.Get(channel + w0 + ":donw").Int()
	if err != nil {
		//log.Println("Conn.Get err in clean: ", err)
		return 0
	}
	if res >= count {
		c.Conn.Del(channel + w0, channel + w0 + ":done")
		left := (*waiting)[0]
		*waiting = (*waiting)[1:]
		return left.Size()
	}
	return 0
}

func (c *Client) ProcessLogsFromRedis(id int, callback func(string)) {
	for {
		fdata := c.FetchPendingMessage(strconv.Itoa(id))

		for _, v := range fdata {
			ch := v.ChatId
			for _, message := range v.ChatMessages {
				logfile := message.Message

				if logfile == ":done" {
					return
				} else if logfile == "" {
					continue
				}

				blockReader := c.readBlocks
				if strings.HasSuffix(logfile, ".gz") {
					blockReader = c.readBlocksGz
				}

				for line := range c.readLines(ch + logfile, blockReader) {
					if line == "" {
						break
					}
					callback(line)
				}
				callback("")

				c.Conn.Incr(ch+logfile + ":done")
			}
		}

		if len(fdata) == 0 {
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Client) readLines(key string, rblocks func(string) <- chan string) <- chan string {
	res := make(chan string)
	go func() {
		var out string
		for block := range rblocks(key) {
			out += block
			posn := strings.LastIndex(out, "\n")
			if posn >= 0 {
				for _, line := range strings.Split(out[:posn], "\n") {
					res <- line + "\n"
				}
				out = out[posn + 1:]
			}
		}
		res <- out
		defer close(res)
	}()
	return res
}

func (c *Client) readBlocks(key string) <- chan string {
	blocksize := int64(math.Pow(2, 17))
	res := make(chan string)
	go func() {
		var lb = blocksize
		var pos int64 = 0
		for lb == blocksize {
			block := c.Conn.GetRange(key, pos, pos + blocksize - 1).Val()
			res <- block
			lb = int64(len(block))
			pos += lb
		}
		defer close(res)
	}()

	return res
}

func (c *Client) readBlocksGz(key string) <- chan string {
	res := make(chan string)
	go func() {
		for temp := range c.readBlocks(key) {
			blockreader, err := gzip.NewReader(bytes.NewReader([]byte(temp)))
			block, _ := ioutil.ReadAll(blockreader)
			if err != nil {
				log.Println("gzip reader err in readBlocksGz: ", err)
				continue
			}
			res <- string(block)
		}
		defer close(res)
	}()

	return res
}

//TODO: achieve the func DailyCountryAggregate
//func (c *Client) DailyCountryAggregate(line string) {
//}
