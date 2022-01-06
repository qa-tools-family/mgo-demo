package main

import (
	"context"
	"github.com/qiniu/qmgo"
	"github.com/qiniu/qmgo/field"
	"github.com/qiniu/qmgo/middleware"
	"github.com/qiniu/qmgo/operator"
	logger "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
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
	//Id     string `bson:"_id"`
	Name   string `bson:"name"`
	Age    uint16 `bson:"age"`
	Weight uint32 `bson:"weight"`
}

type ResultUserInfo struct {
	UserInfo
	Id string `bson:"_id"`
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

func BatchAddData(ctx context.Context, c *qmgo.QmgoClient) error {
	var userInfos = []UserInfo{
		{Name: "a1", Age: 6, Weight: 20},
		{Name: "b2", Age: 6, Weight: 25},
		{Name: "c3", Age: 6, Weight: 30},
		{Name: "d4", Age: 6, Weight: 35},
		{Name: "a1", Age: 7, Weight: 40},
		{Name: "a1", Age: 8, Weight: 45},
	}
	result, err := c.InsertMany(ctx, userInfos)
	if err != nil {
		logger.Error("batch add data failed: ", err)
	}
	// 将插入结果转化为ObjectID的字符串格式
	var objectids []string
	for _, id := range result.InsertedIDs {
		objectids = append(objectids, id.(primitive.ObjectID).Hex())
	}
	logger.Info("batch add data ids: ", objectids)
	return err
}

func BatchGetData(ctx context.Context, c *qmgo.QmgoClient) error {
	var userInfos []UserInfo
	err := c.Find(ctx, bson.M{}).Sort("age").Skip(40).Limit(5).All(&userInfos)
	if err != nil {
		logger.Error("batch get data failed: ", err)
	}
	for _, userInfo := range userInfos {
		logger.Info("get user info", userInfo)
	}
	return err
}

func GetCount(ctx context.Context, c *qmgo.QmgoClient) error {
	number, err := c.Find(ctx, bson.M{}).Count()
	if err != nil {
		logger.Error("get count failed: ", err)
	}
	logger.Info("get count result: ", number)
	return err
}

func UpdateOneData(ctx context.Context, c *qmgo.QmgoClient) error {
	err := c.UpdateOne(ctx, bson.M{}, bson.M{"$set": bson.M{"age": 10}})
	//err := c.UpdateOne(ctx, bson.M{}, bson.M{"$set": UserInfo{Name: "xxx"}})
	if err != nil {
		logger.Error("update data failed: ", err)
	}
	return err
}

func UpdateManyData(ctx context.Context, c *qmgo.QmgoClient) error {
	result, err := c.UpdateAll(ctx, bson.M{}, bson.M{"$set": bson.M{"age": 10}})
	if err != nil {
		logger.Error("update data failed: ", err)
	}
	logger.Info(result.MatchedCount, result.ModifiedCount, result.UpsertedCount, result.UpsertedID)
	return err
}

func SelectFieldResult(ctx context.Context, c *qmgo.QmgoClient) error {
	var userInfos []UserInfo
	err := c.Find(ctx, bson.M{}).Select(bson.M{"name": 1}).All(&userInfos)
	if err != nil {
		logger.Error("update data failed: ", err)
	}
	for _, userInfo := range userInfos {
		logger.Info("get user info", userInfo)
	}
	return err
}

func PipelineFunction(ctx context.Context, c *qmgo.QmgoClient) error {
	matchStage := bson.D{{"$match", []bson.E{{"weight", bson.D{{"$gt", 30}}}}}}
	groupStage := bson.D{{"$group", bson.D{{"_id", "$name"}, {"total", bson.D{{"$sum", "$age"}}}}}}
	var showsWithInfo []bson.M
	err := c.Aggregate(ctx, qmgo.Pipeline{matchStage, groupStage}).All(&showsWithInfo)
	logger.Info("Pipeline result: ", showsWithInfo)
	return err
}

func ExecuteTransaction(ctx context.Context, c *qmgo.QmgoClient) error {
	callback := func(sessCtx context.Context) (interface{}, error) {
		// 重要：确保事务中的每一个操作，都使用传入的sessCtx参数
		if _, err := c.InsertOne(sessCtx, bson.D{{"abc", int32(1)}}); err != nil {
			return nil, err
		}
		if _, err := c.InsertOne(sessCtx, bson.D{{"xyz", int32(999)}}); err != nil {
			return nil, err
		}
		return nil, nil
	}
	result, err := c.DoTransaction(ctx, callback)
	if err != nil {
		logger.Error("transaction execute failed: ", err)
		return err
	}
	logger.Info("result: ", result)
	return nil
}

func UsePreDefineOperator(ctx context.Context, c *qmgo.QmgoClient) error {
	matchStage := bson.D{{operator.Match, []bson.E{{"weight", bson.D{{operator.Gt, 30}}}}}}
	groupStage := bson.D{{operator.Group, bson.D{{"_id", "$name"}, {"total", bson.D{{operator.Sum, "$age"}}}}}}
	var showsWithInfo []bson.M
	err := c.Aggregate(ctx, qmgo.Pipeline{matchStage, groupStage}).All(&showsWithInfo)
	logger.Info("Pipeline result: ", showsWithInfo)
	return err
}


type User struct {
	Name         string    `bson:"name"`
	Age          int       `bson:"age"`
}

func (u *User) BeforeInsert(ctx context.Context) error {
	logger.Info("before insert called")
	return nil
}

func (u *User) AfterInsert(ctx context.Context) error {
	logger.Info("after insert called")
	return nil
}

func HookDemoFunction(ctx context.Context, c *qmgo.QmgoClient) error {
	u := &User{Name: "Alice", Age: 7}
	_, err := c.InsertOne(ctx, u)
	return err
}

type User2 struct {
	field.DefaultField `bson:",inline"`
	Name string `bson:"name"`
	Age  int    `bson:"age"`
}

func SupplyDefaultField(ctx context.Context, c *qmgo.QmgoClient) error {
	u := &User2{Name: "Alice22", Age: 17}
	_, err := c.InsertOne(ctx, u)
	if err != nil {
		logger.Error("Insert Data failed: ", err)
	}
	return err
}

type User3 struct {
	Name string `bson:"name"`
	Age  int    `bson:"age"`

	MyId         string    `bson:"myId"`
	CreateTimeAt time.Time `bson:"createTimeAt"`
	UpdateTimeAt int64     `bson:"updateTimeAt"`
}

func (u *User3) CustomFields() field.CustomFieldsBuilder {
	return field.NewCustom().SetCreateAt("CreateTimeAt").SetUpdateAt("UpdateTimeAt").SetId("MyId")
}

func SupplyCustomField(ctx context.Context, c *qmgo.QmgoClient) error {
	u := &User3{Name: "Alice22", Age: 17}
	_, err := c.InsertOne(ctx, u)
	if err != nil {
		logger.Error("Insert Data failed: ", err)
	}
	return err
}

func QueryUser2Data(ctx context.Context, c *qmgo.QmgoClient) error {
	u := User2{}
	err := c.Find(ctx, bson.M{}).One(&u)
	if err != nil {
		logger.Error("Query User3 Data failed: ", err)
		return err
	}
	logger.Info("User3 info: ", u)
	return err
}

type NewUser struct {
	FirstName string            `bson:"fname"`
	LastName  string            `bson:"lname"`
	Age       uint8             `bson:"age" validate:"gte=0,lte=130" `    // Age must in [0,130]
	Email     string            `bson:"e-mail" validate:"required,email"` //  Email can't be empty string, and must has email format
	CreateAt  time.Time         `bson:"createAt" validate:"lte"`          // CreateAt must lte than current time
	Relations map[string]string `bson:"relations" validate:"max=2"`       // Relations can't has more than 2 elements
}

func FieldValidateFunction(ctx context.Context, c *qmgo.QmgoClient) error {
	u := &NewUser{FirstName: "Alice", Age: 17, Email: "123"}
	_, err := c.InsertOne(ctx, u)
	if err != nil {
		logger.Error("Insert Data failed: ", err)
	}
	return err
}

func Docallback(ctx context.Context, doc interface{}, opType operator.OpType, opts ...interface{}) error {
	if doc.(*UserInfo).Name == "Helloworld" && opType == operator.BeforeInsert {
		logger.Info("before insert success, doc: ", doc)
		return nil
	}
	return nil
}

func MiddlewareDemoFunction(ctx context.Context, c *qmgo.QmgoClient) error {
	middleware.Register(Docallback)
	_, err := c.InsertOne(ctx, &UserInfo{Name: "Helloworld"})
	_, err = c.InsertOne(ctx, &UserInfo{Name: "Helloworld1"})
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
	//_ = BatchRemoveData(ctx, client)
	//_ = BatchAddData(ctx, client)
	//_ = BatchGetData(ctx, client)
	//_ = GetCount(ctx, client)
	//_ = UpdateOneData(ctx, client)
	//_ = UpdateManyData(ctx, client)
	//_ = SelectFieldResult(ctx, client)
	//_ = PipelineFunction(ctx, client)
	//_ = ExecuteTransaction(ctx, client)
	//_ = UsePreDefineOperator(ctx, client)
	//_ = HookDemoFunction(ctx, client)
	//_ = SupplyDefaultField(ctx, client)
	//_ = SupplyCustomField(ctx, client)
	_ = QueryUser2Data(ctx, client)
	//_ = MiddlewareDemoFunction(ctx, client)
	//_ = FieldValidateFunction(ctx, client)
}
