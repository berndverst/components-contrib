/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package documentstores

import (
	"context"
	"fmt"

	"github.com/dapr/components-contrib/health"
)

// Store is an interface to perform operations on store.
type DocumentStore interface {
	Init(metadata Metadata) error
	Features() []Feature

	// HTTP DELETE
	DeleteDocument(ctx context.Context, req *DeleteDocumentRequest) error

	// HTTP GET
	GetDocument(ctx context.Context, req *GetDocumentRequest) (*GetDocumentResponse, error)

	// HTTP GET
	ListDocuments(ctx context.Context, req *ListDocumentRequest) (*ListDocumentResponse, error)

	// HTTP POST
	CreateDocument(ctx context.Context, req *CreateDocumentRequest) (*CreateDocumentResponse, error)
	// HTTP PUT
	ReplaceDocument(ctx context.Context, req *ReplaceDocumentRequest) (*ReplaceDocumentResponse, error)

	// should this be a bulk operation? or update a single document by ID?
	// If bulk, should it require an array of IDs, or should it perform a query?
	// HTTP PATCH
	UpdateDocument(ctx context.Context, req *UpdateDocumentRequest) (*UpdateDocumentResponse, error)

	// HTTP POST
	QueryDocuments(ctx context.Context, req *QueryDocumentRequest) (*QueryDocumentResponse, error)

	GetComponentMetadata() map[string]string
}

func Ping(store DocumentStore) error {
	// checks if this store has the ping option then executes
	if storeWithPing, ok := store.(health.Pinger); ok {
		return storeWithPing.Ping()
	} else {
		return fmt.Errorf("ping is not implemented by this document store")
	}
}
