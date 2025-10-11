package main

import (
	"bufio"
	"context"
	"fmt"
	proto "grpc-client/proto"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
)

func main() {
	file, err := os.Open("/home/michael/Downloads/Sparky/203398809_10215736609888198_4937431234587046278_n.jpg")

	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	bs := make([]byte, stat.Size())
	_, err = bufio.NewReader(file).Read(bs)
	if err != nil && err != io.EOF {
		fmt.Println(err)
		return
	}

	start := time.Now()

	var opts []grpc.DialOption

	opts = append(opts)

	conn, err := grpc.NewClient("localhost:5050", opts...)

	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	defer conn.Close()

	client := proto.NewScanImageClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	res, err := client.ScanImage(ctx, &proto.ScanImageRequest{
		Id:    "hello",
		User:  "bar",
		Image: bs,
	})

	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	elapsed := time.Since(start)

	fmt.Println(elapsed)

	fmt.Println(res)
}
