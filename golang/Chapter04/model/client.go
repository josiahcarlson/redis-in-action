package model

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"log"
	"time"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func (c *Client) ListItem(itemid, sellerid string, price float64) bool {
	inventory := fmt.Sprintf("inventory:%s", sellerid)
	item := fmt.Sprintf("%s.%s", itemid, sellerid)
	end := time.Now().Unix() + 5

	for time.Now().Unix() < end {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			if _, err := tx.TxPipelined(func(pipeliner redis.Pipeliner) error {
				if !tx.SIsMember(inventory, itemid).Val() {
					tx.Unwatch(inventory)
					return nil
				}
				pipeliner.ZAdd("market:", &redis.Z{Member: item, Score: price})
				pipeliner.SRem(inventory, itemid)
				return nil
			}); err != nil {
				return err
			}
			return nil
		}, inventory)

		if err != nil {
			log.Println("watch err: ", err)
			return false
		}
		return true
	}
	return false
}

func (c *Client) PurchaseItem(buyerid, itemid, sellerid string, lprice int64) bool {
	buyer := fmt.Sprintf("users:%s", buyerid)
	seller := fmt.Sprintf("users:%s", sellerid)
	item := fmt.Sprintf("%s.%s", itemid, sellerid)
	inventory := fmt.Sprintf("inventory:%s", buyerid)
	end := time.Now().Unix() + 10

	for time.Now().Unix() < end {
		err := c.Conn.Watch(func(tx *redis.Tx) error {
			if _, err := tx.TxPipelined(func(pipeliner redis.Pipeliner) error {
				price := int64(pipeliner.ZScore("market:", item).Val())
				funds, _ := tx.HGet(buyer, "funds").Int64()
				if price != lprice || price > funds {
					tx.Unwatch()
				}

				pipeliner.HIncrBy(seller, "funds", price)
				pipeliner.HIncrBy(buyer, "funds", -price)
				pipeliner.SAdd(inventory, itemid)
				pipeliner.ZRem("market:", item)
				return nil
			}); err != nil {
				return err
			}
			return nil
		}, "market:", buyer)
		if err != nil {
			log.Println(err)
			return false
		}
		return true
	}
	return false
}

func (c *Client) UpdateToken(token, user, item string) {
	timestamp := float64(time.Now().UnixNano())
	c.Conn.HSet("login:", token, user)
	c.Conn.ZAdd("recent:", &redis.Z{Member:token, Score:timestamp})
	if item != "" {
		c.Conn.ZAdd("viewed:" + token, &redis.Z{Member:item, Score:timestamp})
		c.Conn.ZRemRangeByRank("viewed:" + token, 0, -26)
		c.Conn.ZIncrBy("viewed:", -1, item)
	}
}

func (c *Client) UpdateTokenPipeline (token, user, item string) {
	timestamp := float64(time.Now().UnixNano())
	pipe := c.Conn.Pipeline()
	pipe.HSet("login:", token, user)
	pipe.ZAdd("recent:", &redis.Z{Member:token, Score:timestamp})
	if item != "" {
		pipe.ZAdd("viewed:" + token, &redis.Z{Member:item, Score:timestamp})
		pipe.ZRemRangeByRank("viewed:" + token, 0, -26)
		pipe.ZIncrBy("viewed:", -1, item)
	}
	if _, err := pipe.Exec(); err != nil {
		log.Println("pipeline err in UpdateTokenPipeline: ", err)
		return
	}
	return
}

//TODO: lack the parts before 4.4

