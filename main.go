package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/arl/statsviz"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/panjf2000/ants/v2"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"time"
	"trunc/trunc"
)

//import "github.com/aws/aws-sdk-go-v2/service/dynamodb"

const (
	AwsRegion = "us-east-1"
	CoreMultiplier = 4
)

var (
	cfg aws.Config
	mux = http.NewServeMux()
	client *dynamodb.Client
	pool *ants.Pool
	awsRegion = os.Getenv("AWS_REGION")
	tableName string
	pscan int
	limit int
	queueSize int
	maxRetries int
	poolSize int

	cores = runtime.NumCPU()
)

func main() {
	stats()
	if awsRegion == "" {
		awsRegion = AwsRegion
	}

	// client *dynamodb.Client, pool *ants.Pool, tableName string, pscan, limit, queue, maxretries int
	flag.StringVar(&tableName, "table", "", "The table name to truncate")
	flag.StringVar(&awsRegion, "region", awsRegion, "The AWS Region")
	flag.IntVar(&pscan, "segments", cores * CoreMultiplier, "The number of parallel scan segments")
	flag.IntVar(&limit, "limit", 1000, "The parallel scan limit")
	flag.IntVar(&queueSize, "queue", 1024 * 10, "The processing channel size")
	flag.IntVar(&maxRetries, "retries", 64, "The maximum number of retries")
	flag.IntVar(&poolSize, "pool", cores * CoreMultiplier, "The go routine pool size")
	flag.Parse()
	if tableName == "" {
		log.Fatal("No table specified")
	}
	log.Printf("Truncating DynamoDB Table: table=%s, region=%s, segments=%d, limit=%d, channel=%d, retries=%d\n", tableName, awsRegion, pscan, limit, queueSize, maxRetries)
	var err error
	cfg, err = config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	log.Printf("SDK Config Loaded")
	pool, err = ants.NewPool(poolSize, ants.WithPreAlloc(true))

	if err != nil {
		log.Fatal("Failed to build go routine pool: ", err.Error())
	}

	client = dynamodb.NewFromConfig(cfg)
	// client *dynamodb.Client, pool *ants.Pool, tableName string, pscan, limit, queue, maxretries int
	truncator, err := trunc.NewTruncator(client, pool, tableName, pscan, limit, queueSize, maxRetries)
	if err != nil {
		log.Fatal("Failed to build truncator: ", err.Error())
	}
	log.Printf("Created %s\n", truncator.String())
	truncator.Start()
	time.Sleep(time.Hour)

}

func stats() {
	statsviz.Register(mux)
	go func() {
		listener, _ := net.Listen("tcp", ":0")
		port := listener.Addr().(*net.TCPAddr).Port
		log.Printf("StatsViz URL: http://localhost:%d/debug/statsviz/\n", port)
		listenSocket := fmt.Sprintf("localhost:%d", port)
		log.Println(http.ListenAndServe(listenSocket, mux))
	}()
}