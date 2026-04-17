// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// OSClient wraps the opensearch-go v4 client.
type OSClient struct {
	client *opensearchapi.Client
	index  string
}

// osSource is the shape of the _source field in the resources index.
type osSource struct {
	Latest bool           `json:"latest"`
	V1Data map[string]any `json:"data"`
}

// OSDoc is the result of a GetByID call.
type OSDoc struct {
	Found  bool
	Latest bool
	V1Data map[string]any
}

// NewOSClient creates an OpenSearch client using URL-only auth.
func NewOSClient(url, index string) (*OSClient, error) {
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{url},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating opensearch client: %w", err)
	}
	return &OSClient{client: client, index: index}, nil
}

// GetByID fetches a single document. Returns (doc, nil) when found,
// ({Found:false}, nil) on 404, and (nil, err) on transport errors.
func (c *OSClient) GetByID(ctx context.Context, id string) (*OSDoc, error) {
	resp, err := c.client.Document.Get(ctx, opensearchapi.DocumentGetReq{
		Index:      c.index,
		DocumentID: id,
	})
	if err != nil {
		var strErr *opensearch.StringError
		if errors.As(err, &strErr) && strErr.Status == http.StatusNotFound {
			return &OSDoc{Found: false}, nil
		}
		return nil, fmt.Errorf("opensearch get %q: %w", id, err)
	}

	if !resp.Found {
		return &OSDoc{Found: false}, nil
	}

	var src osSource
	if err := json.Unmarshal(resp.Source, &src); err != nil {
		return nil, fmt.Errorf("unmarshalling _source for %q: %w", id, err)
	}

	return &OSDoc{
		Found:  true,
		Latest: src.Latest,
		V1Data: src.V1Data,
	}, nil
}
