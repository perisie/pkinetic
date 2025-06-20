package pkinetic

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strings"
)

type Pkinetic_dynamo struct {
	dynamo *dynamodb.Client
	table  string
}

func (p *Pkinetic_dynamo) Delete(partition_key string, sort_key string) error {
	_, err := p.dynamo.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(p.table),
		Key: map[string]types.AttributeValue{
			"partition_key": &types.AttributeValueMemberS{
				Value: partition_key,
			},
			"sort_key": &types.AttributeValueMemberS{
				Value: sort_key,
			},
		},
	})
	return err
}

func (p *Pkinetic_dynamo) Update(partition_key string, sort_key string, update map[string]string) error {
	updates := []string{}
	attribute_names := map[string]string{}
	attribute_values := map[string]types.AttributeValue{}
	for k, v := range update {
		updates = append(updates, fmt.Sprintf("#%s = :%s", k, k))
		attribute_names["#"+k] = k
		attribute_values[":"+k] = &types.AttributeValueMemberS{
			Value: v,
		}
	}
	_, err := p.dynamo.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName: aws.String(p.table),
		Key: map[string]types.AttributeValue{
			"partition_key": &types.AttributeValueMemberS{
				Value: partition_key,
			},
			"sort_key": &types.AttributeValueMemberS{
				Value: sort_key,
			},
		},
		UpdateExpression:          aws.String("SET " + strings.Join(updates, ",")),
		ExpressionAttributeNames:  attribute_names,
		ExpressionAttributeValues: attribute_values,
		ConditionExpression:       aws.String("attribute_exists(partition_key)"),
	})
	return err
}

func (p *Pkinetic_dynamo) Get(partition_key string, prefix string) ([]*Item, error) {
	items := make([]*Item, 0)
	query_output, err := p.dynamo.Query(context.Background(), &dynamodb.QueryInput{
		TableName:              aws.String(p.table),
		KeyConditionExpression: aws.String("partition_key = :partition_key AND begins_with(sort_key, :prefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":partition_key": &types.AttributeValueMemberS{
				Value: partition_key,
			},
			":prefix": &types.AttributeValueMemberS{
				Value: prefix,
			},
		},
	})
	if err != nil {
		return items, err
	}
	for _, item := range query_output.Items {
		data := map[string]string{}
		for k, v := range item {
			data[k] = v.(*types.AttributeValueMemberS).Value
		}
		items = append(items, &Item{
			partition_key: data["partition_key"],
			sort_key:      data["sort_key"],
			data:          data,
		})
	}
	return items, nil
}

func (p *Pkinetic_dynamo) Create(partition_key string, sort_key string, data map[string]string) error {
	item := map[string]types.AttributeValue{
		"partition_key": &types.AttributeValueMemberS{
			Value: partition_key,
		},
		"sort_key": &types.AttributeValueMemberS{
			Value: sort_key,
		},
	}
	for k, v := range data {
		item[k] = &types.AttributeValueMemberS{
			Value: v,
		}
	}
	_, err := p.dynamo.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName:           aws.String(p.table),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(partition_key)"),
	})
	return err
}

func Pkinetic_dynamo_new(
	region string,
	table string,
) (*Pkinetic_dynamo, error) {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	dynamo := dynamodb.NewFromConfig(cfg)

	return &Pkinetic_dynamo{
		table:  table,
		dynamo: dynamo,
	}, nil
}
