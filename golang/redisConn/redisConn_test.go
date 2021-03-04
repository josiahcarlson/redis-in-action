package redisConn

import (
	"github.com/go-redis/redis/v7"
	"redisInAction/utils"
	"testing"
)

func TestCheckVersion(t *testing.T) {
	version, err := CheckVersion("6.0.9", "4.0.0")
	utils.AssertTrue(t, err == nil)
	utils.AssertStringResult(t, "6.0.9", version)

	version, err = CheckVersion("3.0", "4.0.0")
	utils.AssertTrue(t, err != nil)
	utils.AssertStringResult(t, "", version)
}

// Demonstrate how to use redis transaction pipeline with go-redis
func TestTxPipeline(t *testing.T) {
	conn := ConnectRedis()
	client := NewClient(conn)

	t.Run("Tx Example1: tx.TxPipeline()", func(t *testing.T) {
		err := client.Conn.Watch(func(tx *redis.Tx) error {
			t.Log("before tx", client.Conn.Incr("key"))
			pipe := tx.TxPipeline()
			t.Log("inside tx", pipe.Incr("key"))
			if _, err := pipe.Exec(); err != nil {
				return err
			}
			return nil
		}, "key")
		utils.AssertTrue(t, err != nil)
		utils.AssertStringResult(t, "redis: transaction failed", err.Error())

		cmd := client.Conn.Get("key")
		t.Log("after tx", cmd)
		utils.AssertTrue(t, cmd.Val() == "1")

		defer client.Conn.FlushDB()
	})

	t.Run("Tx Example2: tx.Pipelined()", func(t *testing.T) {
		err := client.Conn.Watch(func(tx *redis.Tx) error {
			t.Log("before tx", client.Conn.Incr("key"))
			_, err := tx.Pipelined(func(pipe redis.Pipeliner) error {
				t.Log("inside tx", pipe.Incr("key"))
				return nil
			})
			return err
		}, "key")
		utils.AssertTrue(t, err != nil)
		utils.AssertStringResult(t, "redis: transaction failed", err.Error())

		cmd := client.Conn.Get("key")
		t.Log("after tx", cmd)
		utils.AssertTrue(t, cmd.Val() == "1")

		defer client.Conn.FlushDB()
	})

	t.Run("Tx a wrong example", func(t *testing.T) {
		pipe := client.Conn.TxPipeline()
		err := client.Conn.Watch(func(tx *redis.Tx) error {
			cmd := client.Conn.Incr("key")
			t.Log("before tx", cmd)
			t.Log("inside tx", pipe.Incr("key"))
			if _, err := pipe.Exec(); err != nil {
				return err
			}
			return nil
		}, "key")

		// WATCH not work!
		utils.AssertTrue(t, err == nil)

		cmd := client.Conn.Get("key")
		t.Log("after tx", cmd)
		utils.AssertTrue(t, cmd.Val() == "2")

		defer client.Conn.FlushDB()
	})
}
