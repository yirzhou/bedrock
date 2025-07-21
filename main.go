package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"wal/bedrock"
)

func main() {
	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Println("Please specify the root directory for WAL and checkpoints.")
		return
	}
	dbDir := args[0]
	checkpointSize, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		fmt.Println("Please specify the checkpoint size as an integer.")
		return
	}

	dbConfig := bedrock.NewDefaultConfiguration().WithBaseDir(dbDir).WithCheckpointSize(checkpointSize)
	kv, err := bedrock.Open(dbConfig)
	if err != nil {
		log.Println("Error creating KVStore:", err)
		return
	}
	defer kv.Close()
	kv.Print()

	// for i := 0; i < 100; i++ {
	// 	kv.Put([]byte(fmt.Sprintf("key-%d", i)), []byte(fmt.Sprintf("value-%d", i)))
	// }

	// kv.Put([]byte(""), []byte("value2"))
	// kv.Put([]byte("color"), []byte(""))
	// kv.Put([]byte("key-1"), []byte("some utf-8 chars ✨ or binary data \x00\x01\x02"))

}
