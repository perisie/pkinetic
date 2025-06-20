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
	key_condition_expression := "partition_key = :partition_key"
	expression_attribute_values := map[string]types.AttributeValue{
		":partition_key": &types.AttributeValueMemberS{
			Value: partition_key,
		},
	}
	if prefix != "" {
		key_condition_expression += " AND begins_with(sort_key, :prefix)"
		expression_attribute_values[":prefix"] = &types.AttributeValueMemberS{
			Value: prefix,
		}
	}
	query_output, err := p.dynamo.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                 aws.String(p.table),
		KeyConditionExpression:    aws.String(key_condition_expression),
		ExpressionAttributeValues: expression_attribute_values,
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

func (p *Pkinetic_dynamo) Get_gsi(
	index string,
	index_partition_key_name string,
	index_partition_key_value string,
	index_sort_key_name string,
	index_sort_key_prefix string,
) ([]*Item, error) {
	items := make([]*Item, 0)
	key_condition_expression := "#pk = :index_partition_key"
	expression_attribute_names := map[string]string{
		"#pk": index_partition_key_name,
	}
	expression_attribute_values := map[string]types.AttributeValue{
		":index_partition_key": &types.AttributeValueMemberS{
			Value: index_partition_key_value,
		},
	}
	if len(index_sort_key_prefix) > 0 {
		key_condition_expression += " AND begins_with(#sk, :index_sort_key_prefix)"
		expression_attribute_names["#sk"] = index_sort_key_name
		expression_attribute_values[":index_sort_key_prefix"] = &types.AttributeValueMemberS{
			Value: index_sort_key_prefix,
		}
	}
	query_output, err := p.dynamo.Query(context.Background(), &dynamodb.QueryInput{
		TableName:                 aws.String(p.table),
		IndexName:                 aws.String(index),
		KeyConditionExpression:    aws.String(key_condition_expression),
		ExpressionAttributeNames:  expression_attribute_names,
		ExpressionAttributeValues: expression_attribute_values,
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

func (p *Pkinetic_dynamo) Get_single(partition_key string, sort_key string) (*Item, error) {
	get_item_output, err := p.dynamo.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String(p.table),
		Key: map[string]types.AttributeValue{
			"partition_key": &types.AttributeValueMemberS{
				Value: partition_key,
			},
			"sort_key": &types.AttributeValueMemberS{
				Value: sort_key,
			},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	data := map[string]string{}
	for k, v := range get_item_output.Item {
		data[k] = v.(*types.AttributeValueMemberS).Value
	}
	item := &Item{
		partition_key: data["partition_key"],
		sort_key:      data["sort_key"],
		data:          data,
	}
	return item, nil
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
