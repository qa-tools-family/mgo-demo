package main

import (
	"context"
	"fmt"
	"github.com/qiniu/qmgo"
	qoptions "github.com/qiniu/qmgo/options"
	logger "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)


func getCollectionByBasic(ctx context.Context, mongostr string) *qmgo.Collection {
	var maxNumber uint64 = 100
	var minNumber uint64 = 20
	client, err := qmgo.NewClient(ctx, &qmgo.Config{Uri: mongostr, MaxPoolSize: &maxNumber, MinPoolSize: &minNumber})
	if err != nil {
		logger.Errorf("connect to mongodb failed: %s", err)
	}
	db := client.Database("class")
	coll := db.Collection("user")
	return coll
}

func getDBClientByOpen(ctx context.Context, mongostr string) *qmgo.QmgoClient {
	poolMonitor := &event.PoolMonitor{
		Event: func(evt *event.PoolEvent) {
			switch evt.Type {
			case event.GetSucceeded:
				fmt.Println("GetSucceeded")
			case event.ConnectionReturned:
				fmt.Println("ConnectionReturned")
			case event.ConnectionClosed:
				fmt.Println("ConnectionClosed")
			case event.PoolCreated:
				fmt.Println("PoolCreated")
			}
		},
	}
	opt := options.Client().SetPoolMonitor(poolMonitor)
	cli, err := qmgo.Open(ctx, &qmgo.Config{
		Uri: mongostr,
		Database: "class",
	}, qoptions.ClientOptions{ClientOptions: opt})
	if err != nil {
		logger.Errorf("connect to mongodb failed: %s", err)
	}
	return cli
}

func getCollClientByOpen(ctx context.Context, mongostr string) *qmgo.QmgoClient {
	cli, err := qmgo.Open(ctx, &qmgo.Config{
		Uri: mongostr,
		Database: "class",
		Coll: "user",
	})
	if err != nil {
		logger.Errorf("connect to mongodb failed: %s", err)
	}
	return cli
}


func main()  {
	ctx := context.Background()

	//coll := getCollectionByBasic(ctx, "mongodb://localhost:27017")
	//logger.Info("collection name: ", coll.GetCollectionName())

	client := getDBClientByOpen(ctx, "mongodb://localhost:27017")
	logger.Info("collection name: ", client.Database.Collection("user").GetCollectionName())
	logger.Info("collection name: ", client.Database.Collection("room").GetCollectionName())
	_ = client.Close(ctx)
	//
	//collClient := getCollClientByOpen(ctx, "mongodb://localhost:27017")
	//logger.Info("collection name: ", collClient.GetCollectionName())

	time.Sleep(3 * time.Second)
}
