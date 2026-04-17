// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoClient wraps the AWS DynamoDB client.
type DynamoClient struct {
	client *dynamodb.Client
}

// NewDynamoClient constructs a DynamoDB client using the standard AWS SDK
// credential chain. Set AWS_PROFILE and AWS_SDK_LOAD_CONFIG=1 to use a
// named profile (including its configured region and assume-role settings).
func NewDynamoClient(ctx context.Context) (*DynamoClient, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	return &DynamoClient{client: dynamodb.NewFromConfig(awsCfg)}, nil
}

// ScanTable pages through the table and calls fn for each item.
// If projectAttr and projectUID are both non-empty a FilterExpression is applied.
// Iteration stops after limit items (0 = unlimited).
func (d *DynamoClient) ScanTable(
	ctx context.Context,
	table string,
	projectAttr string,
	projectUID string,
	limit int,
	fn func(item map[string]any) error,
) (int, error) {
	input := &dynamodb.ScanInput{
		TableName: aws.String(table),
	}

	if projectAttr != "" && projectUID != "" {
		input.FilterExpression = aws.String("#proj = :proj")
		input.ExpressionAttributeNames = map[string]string{
			"#proj": projectAttr,
		}
		input.ExpressionAttributeValues = map[string]types.AttributeValue{
			":proj": &types.AttributeValueMemberS{Value: projectUID},
		}
	}

	paginator := dynamodb.NewScanPaginator(d.client, input)
	scanned := 0

	for paginator.HasMorePages() {
		if ctx.Err() != nil {
			return scanned, ctx.Err()
		}

		page, err := paginator.NextPage(ctx)
		if err != nil {
			return scanned, fmt.Errorf("scanning %s: %w", table, err)
		}

		for _, rawItem := range page.Items {
			if limit > 0 && scanned >= limit {
				return scanned, nil
			}

			item := make(map[string]any)
			if err := attributevalue.UnmarshalMap(rawItem, &item); err != nil {
				return scanned, fmt.Errorf("unmarshalling item from %s: %w", table, err)
			}

			if err := fn(item); err != nil {
				return scanned, err
			}
			scanned++
		}
	}

	return scanned, nil
}

// ObjectID derives the OpenSearch document ID from a DynamoDB item.
// Single-key tables: objectType + ":" + value of idAttr.
// Composite-key tables: objectType + ":" + values joined by ":".
func ObjectID(objectType string, idAttr string, compositeAttrs []string, item map[string]any) (string, error) {
	if len(compositeAttrs) > 0 {
		parts := make([]string, 0, len(compositeAttrs))
		for _, attr := range compositeAttrs {
			v, ok := item[attr]
			if !ok {
				return "", fmt.Errorf("composite key attribute %q missing from item", attr)
			}
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		return objectType + ":" + strings.Join(parts, ":"), nil
	}

	v, ok := item[idAttr]
	if !ok {
		return "", fmt.Errorf("id attribute %q missing from item", idAttr)
	}
	return fmt.Sprintf("%s:%v", objectType, v), nil
}
