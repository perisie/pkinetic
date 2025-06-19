package pkinetic

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Pkinetic_dynamo struct {
	dynamo *dynamodb.Client
	table  string
}

func (p *Pkinetic_dynamo) Get(partition_key string, prefix string) ([]map[string]string, error) {
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
		fmt.Println(err.Error())
		return nil, err
	}
	fmt.Println(len(query_output.Items))
	return nil, nil
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
