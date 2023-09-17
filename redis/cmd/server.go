package main

import (
	bitcaskgo "bitcask-go"
	"bitcask-go/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*redis.RedisStorage
	server *redcon.Server
	mu     *sync.RWMutex
}

func main() {
	//
	redisData, err := redis.NewRedisStorage(bitcaskgo.DefaultOptions)
	if err != nil {
		panic(err)
	}

	bcServer := &BitcaskServer{
		dbs: make(map[int]*redis.RedisStorage),
		mu:  new(sync.RWMutex),
	}

	bcServer.dbs[0] = redisData

	// server := redcon.NewServer(add)
	bcServer.server = redcon.NewServer(addr, execClientCmd, bcServer.accept, bcServer.Close)
	bcServer.Listen()
}

func (svr *BitcaskServer) Listen() {
	log.Printf("bitcask server running, ready to accept connection")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	clt := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()

	clt.server = nil
	clt.db = svr.dbs[0]

	conn.SetContext(clt)

	return true
}

func (svr *BitcaskServer) Close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		db.Close()
	}
}

// func main() {
// 	conn, err := net.Dial("tcp", "localhost:6379")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// send command to redis
// 	cmd := "set key-1 value-1\r\n"
// 	conn.Write([]byte(cmd))

// 	reader := bufio.NewReader(conn)
// 	res, err := reader.ReadString('\n')
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Print(res)

// }
