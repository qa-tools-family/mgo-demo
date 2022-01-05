package main

import (
	"context"
	"github.com/qiniu/qmgo"
	logger "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

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

type UserInfo struct {
	Name   string `bson:"name"`
	Age    uint16 `bson:"age"`
	Weight uint32 `bson:"weight"`
}

func addItem(ctx context.Context, c *qmgo.QmgoClient) (string, error) {
	userInfo := UserInfo{
		Name:   "xm",
		Age:    7,
		Weight: 40,
	}
	result, err := c.InsertOne(ctx, userInfo)
	if err != nil {
		return "", err
	}
	// 将插入结果转化为ObjectID的字符串格式
	requestId := result.InsertedID.(primitive.ObjectID).Hex()
	return requestId, nil
}

func queryDataById(ctx context.Context, c *qmgo.QmgoClient, objectId string) (*UserInfo, error) {
	realObjectId, err := primitive.ObjectIDFromHex(objectId)
	if err != nil {
		logger.Error("parse object id failed: ", err)
		return nil, err
	}
	userInfo := UserInfo{}
	err = c.Find(ctx, bson.M{"_id": realObjectId}).One(&userInfo)
	if err != nil {
		logger.Error("query data by id failed: ", err)
		return nil, err
	} else {
		return &userInfo, nil
	}
}

func RemoveData(ctx context.Context, c *qmgo.QmgoClient) error {
	err := c.Remove(ctx, bson.M{"age": 7})
	if err != nil {
		logger.Error("remove data failed: ", err)
	}
	return err
}

func BatchRemoveData(ctx context.Context, c *qmgo.QmgoClient) error {
	result, err := c.RemoveAll(ctx, bson.M{"age": 7})
	if err != nil {
		logger.Error("remove data failed: ", err)
	}
	logger.Info("remove item count: ", result.DeletedCount)
	return err
}

func main() {
	ctx := context.Background()
	client := getCollClientByOpen(ctx, "mongodb://localhost:27017")
	//item_id, err := addItem(ctx, client)
	//if err != nil {
	//	logger.Errorf("insert data failed: %v", err)
	//} else {
	//	logger.Info("insert data success: ", item_id)
	//}
	//userInfo, err := queryDataById(ctx, client, "61d57298068fb969452f9e41")
	//if err != nil {
	//	return
	//}
	//logger.Info("query response: ", userInfo)
	//_ = RemoveData(ctx, client)
	_ = BatchRemoveData(ctx, client)
}
