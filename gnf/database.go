package gnf

import (
	"github.com/go-redis/redis"
	"strconv"
)

type DBClient interface {
	DBWrite(key, val string) error
	DBRead(key string) (string, error)
}

// composite version, another impl see my prototype
type RedisClient struct {
	redis.Client
}

func (cli *RedisClient) DBWrite(key string, val string) error {
	err := cli.Set(key, val, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func (cli *RedisClient) DBRead(key string) (string, error) {
	val, err := cli.Get(key).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}

func (wl *Workload) GetRemoteDBClients(phase ExePhase) []DBClient {

	var num int
	if phase == LoadSig {
		num = wl.RemoteDBLoadThreadCount
	} else if phase == RunSig {
		num = wl.RemoteDBRunThreadCount
	} else {
		panic("not a valid phase")
	}
	clients := make([]DBClient, num)

	for i := range clients {
		switch wl.RemoteDB {
		case "redis":
			cli := redis.NewClient(&redis.Options{
				Addr:     wl.RemoteDBHost + ":" + strconv.Itoa(wl.RemoteDBPort),
				Password: wl.RemoteDBPassword,
				DB:       0,
			})
			clients[i] = DBClient(&RedisClient{*cli})
		}
	}
	return clients
}
