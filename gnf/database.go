package gnf

/*
	Database client interface, implementations, and DB related functions

	Discussion: for method DBWrite and DBRead, should they have pointer receivers?

*/

import (
	"github.com/go-redis/redis"
	"strconv"
)

type DBClient interface {
	DBWrite(key, val string) error
	DBRead(key string) (string, error)
}

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

func getRemoteDBClients(wl *Workload, phase ExePhase) []DBClient {

	var num int
	if phase == LoadSig {
		num = wl.RemoteDBLoadThreadCount
	} else {
		num = wl.RemoteDBRunThreadCount
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
