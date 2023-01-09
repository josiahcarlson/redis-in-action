package redisConn

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis/v7"
	goversion "github.com/hashicorp/go-version"
	"log"
	"redisInAction/config"
	"regexp"
)

const (
	REDIS_MIN_VERSION = "4.0.0"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func ConnectRedis() *redis.Client {
	conn := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	if _, err := conn.Ping().Result(); err != nil {
		log.Fatalf("Connect to redis client failed, err: %v\n", err)
	}
	if _, err := CheckServerVersion(conn); err != nil {
		log.Fatalf("Check redis version failed, err: %v\n", err)
	}
	return conn
}

func CheckServerVersion(conn *redis.Client) (version string, err error) {
	cmd := conn.Info("server")
	serverInfo := cmd.Val()
	if len(serverInfo) == 0 {
		err = errors.New(fmt.Sprintf("Get server info failed, err: %v", cmd.Err()))
		return
	}
	r := regexp.MustCompile(`redis_version:((\d+\.)+\d+)`)
	matchSlice := r.FindStringSubmatch(serverInfo)
	if len(matchSlice) < 2 {
		err = errors.New("Regexp not match redis_version")
		return
	}
	version = matchSlice[1]
	return CheckVersion(version, REDIS_MIN_VERSION)
}

func CheckVersion(serverVersion string, minVersion string) (version string, err error)  {
	v1, err := goversion.NewVersion(serverVersion)
	if err != nil {
		err = errors.New(fmt.Sprintf("New version failed from string: %s, err: %v", v1, err))
		return
	}
	v2, err := goversion.NewVersion(minVersion)
	if err != nil {
		err = errors.New(fmt.Sprintf("New version failed from string: %s, err: %v", v2, err))
		return
	}

	if v1.LessThan(v2) {
		err = errors.New(fmt.Sprintf("Server version %s does not meet minimum version requirements: %s", serverVersion, minVersion))
		return
	}

	version = serverVersion
	return
}
