package main

import (
	bitcaskgo "bitcask-go"
	"bitcask-go/redis"
	"bitcask-go/utils"
	"fmt"
	"log"
	"strings"

	"github.com/tidwall/redcon"
)

func newWrongNumberArgsError(cmd string) error {
	return fmt.Errorf("Err wrong number of arguments for '%s'", cmd)
}

type cmdHeader func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHeader{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *redis.RedisStorage
}

func execClientCmd(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	switch command {
	case "quit":
		conn.Close()
	case "ping":
		conn.WriteString("pong")
	default:
		cmdHandler, ok := supportedCommands[command]
		if !ok {
			conn.WriteError(fmt.Sprintf("unsupport command %v", command))
			return
		}

		client, _ := conn.Context().(*BitcaskClient)
		res, err := cmdHandler(client, cmd.Args[1:])
		if err != nil {
			if err == bitcaskgo.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}

			return
		}

		conn.WriteAny(res)
	}

}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberArgsError("set")
	}

	key, value := args[0], args[1]
	log.Printf("Set key:%s Value:%s\n", key, value)
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberArgsError("get")
	}

	value, err := cli.db.Get(args[0])
	log.Printf("Get key:%s Value:%s\n", args[0], value)

	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberArgsError("lpush")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
