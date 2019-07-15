package gnf

/*
   Copyright 2019 NFSB Research Team & Developers

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/*
	Database client interface, implementations, and DB related functions

	only supports redis for now
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
	_, err := cli.Get(key).Result()
	if err != nil {
		return "", err
	}
	return "", nil
}

func getRemoteDBClients(wl *Workload, phase exePhase) []DBClient {

	var num int
	if phase == LoadPhase {
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
		// as discussed in readme, add a switch case here to support a new DB backend
		}
	}
	return clients
}
