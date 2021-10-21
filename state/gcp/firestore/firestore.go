// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/datastore"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/api/option"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

const defaultEntityKind = "DaprState"

// Firestore State Store.
type Firestore struct {
	state.DefaultBulkStore
	client     *datastore.Client
	entityKind string

	logger logger.Logger
}

type firestoreMetadata struct {
	Type                string `json:"type"`
	ProjectID           string `json:"project_id"`
	PrivateKeyID        string `json:"private_key_id"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	ClientID            string `json:"client_id"`
	AuthURI             string `json:"auth_uri"`
	TokenURI            string `json:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url"`
	EntityKind          string `json:"entity_kind"`
}

type StateEntity struct {
	Value string
}

func NewFirestoreStateStore(logger logger.Logger) *Firestore {
	s := &Firestore{logger: logger}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init does metadata and connection parsing.
func (f *Firestore) Init(metadata state.Metadata) error {
	meta, err := getFirestoreMetadata(metadata)
	if err != nil {
		return err
	}
	b, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	opt := option.WithCredentialsJSON(b)
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, meta.ProjectID, opt)
	if err != nil {
		return err
	}

	f.client = client
	f.entityKind = meta.EntityKind

	return nil
}

// Features returns the features available in this state store.
func (f *Firestore) Features() []state.Feature {
	return nil
}

// Get retrieves state from Firestore with a key (Always strong consistency).
func (f *Firestore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	key := req.Key

	entityKey := datastore.NameKey(f.entityKind, key, nil)
	var entity StateEntity
	err := f.client.Get(context.Background(), entityKey, &entity)

	if err != nil && !errors.Is(err, datastore.ErrNoSuchEntity) {
		return nil, err
	} else if errors.Is(err, datastore.ErrNoSuchEntity) {
		return &state.GetResponse{}, nil
	}

	return &state.GetResponse{
		Data: []byte(entity.Value),
	}, nil
}

func (f *Firestore) setValue(req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	var v string
	b, ok := req.Value.([]byte)
	if ok {
		v = string(b)
	} else {
		v, _ = jsoniter.MarshalToString(req.Value)
	}

	entity := &StateEntity{
		Value: v,
	}
	ctx := context.Background()
	key := datastore.NameKey(f.entityKind, req.Key, nil)

	_, err = f.client.Put(ctx, key, entity)

	if err != nil {
		return err
	}

	return nil
}

// Set saves state into Firestore with retry.
func (f *Firestore) Set(req *state.SetRequest) error {
	return state.SetWithOptions(f.setValue, req)
}

func (f *Firestore) Ping() error {
	return nil
}

func (f *Firestore) deleteValue(req *state.DeleteRequest) error {
	ctx := context.Background()
	key := datastore.NameKey(f.entityKind, req.Key, nil)

	err := f.client.Delete(ctx, key)
	if err != nil {
		return err
	}

	return nil
}

// Delete performs a delete operation.
func (f *Firestore) Delete(req *state.DeleteRequest) error {
	return state.DeleteWithOptions(f.deleteValue, req)
}

func getFirestoreMetadata(metadata state.Metadata) (*firestoreMetadata, error) {
	meta := firestoreMetadata{
		EntityKind: defaultEntityKind,
	}
	requiredMetaProperties := []string{
		"type", "project_id", "private_key_id", "private_key", "client_email", "client_id",
		"auth_uri", "token_uri", "auth_provider_x509_cert_url", "client_x509_cert_url",
	}

	for _, k := range requiredMetaProperties {
		if val, ok := metadata.Properties[k]; !ok || len(val) < 1 {
			return nil, fmt.Errorf("error parsing required field: %s", k)
		}
	}

	meta.Type = metadata.Properties["type"]
	meta.ProjectID = metadata.Properties["project_id"]
	meta.PrivateKeyID = metadata.Properties["private_key_id"]
	meta.PrivateKey = metadata.Properties["private_key"]
	meta.ClientEmail = metadata.Properties["client_email"]
	meta.ClientID = metadata.Properties["client_id"]
	meta.AuthURI = metadata.Properties["auth_uri"]
	meta.TokenURI = metadata.Properties["token_uri"]
	meta.AuthProviderCertURL = metadata.Properties["auth_provider_x509_cert_url"]
	meta.ClientCertURL = metadata.Properties["client_x509_cert_url"]

	if val, ok := metadata.Properties["entity_kind"]; ok && val != "" {
		meta.EntityKind = val
	}

	return &meta, nil
}
