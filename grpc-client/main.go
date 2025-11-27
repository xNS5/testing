package main

import (
	"context"
	proto "grpc-client/proto"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// file, err := os.Open("/home/michael/Downloads/Sparky/203398809_10215736609888198_4937431234587046278_n.jpg")

	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// defer file.Close()

	// stat, err := file.Stat()
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }

	// bs := make([]byte, stat.Size())
	// _, err = bufio.NewReader(file).Read(bs)
	// if err != nil && err != io.EOF {
	// 	fmt.Println(err)
	// 	return
	// }

	// start := time.Now()

	// var opts []grpc.DialOption

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{
		"methodConfig": [{
		  "name": [{}],
		  "retryPolicy": {
			  "maxAttempts": 4,
			  "initialBackoff": "2s",
			  "maxBackoff": "10s",
			  "backoffMultiplier": 1.0,
			  "retryableStatusCodes": [ "UNAVAILABLE" ]
		  }
		}]}`)}

	conn, err := grpc.NewClient("localhost:5050", opts...)

	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	defer conn.Close()

	client := proto.NewScanImageClient(conn)

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	// defer cancel()

	_, err = client.ScanImage(context.Background(), &proto.ScanImageRequest{
		Id:   "hello",
		User: "bar",
	})

	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// elapsed := time.Since(start)

	// fmt.Println(elapsed)

	// fmt.Println(res)
}
