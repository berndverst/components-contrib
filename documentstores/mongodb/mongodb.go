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

package mongodb

// mongodb package is an implementation of StateStore interface to perform operations on store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/dapr/components-contrib/documentstores"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	host             = "host"
	username         = "username"
	password         = "password"
	databaseName     = "databaseName"
	collectionName   = "collectionName"
	server           = "server"
	operationTimeout = "operationTimeout"
	params           = "params"
	id               = "_id"
	value            = "value"
	etag             = "_etag"

	defaultTimeout        = 5 * time.Second
	defaultDatabaseName   = "daprStore"
	defaultCollectionName = "daprCollection"

	// mongodb://<username>:<password@<host>/<database><params>
	connectionURIFormatWithAuthentication = "mongodb://%s:%s@%s/%s%s"

	// mongodb://<host>/<database><params>
	connectionURIFormat = "mongodb://%s/%s%s"

	// mongodb+srv://<server>/<params>
	connectionURIFormatWithSrv = "mongodb+srv://%s/%s"

	// mongodb+srv://<username>:<password>@<server>/<params>
	connectionURIFormatWithSrvAndCredentials = "mongodb+srv://%s:%s@%s/%s%s" //nolint:gosec
)

// MongoDB is a state store implementation for MongoDB.
type MongoDB struct {
	state.DefaultBulkStore
	client           *mongo.Client
	collection       *mongo.Collection
	operationTimeout time.Duration
	metadata         mongoDBMetadata

	features []documentstores.Feature
	logger   logger.Logger
}

type mongoDBMetadata struct {
	Host             string
	Username         string
	Password         string
	DatabaseName     string
	CollectionName   string
	Server           string
	Writeconcern     string
	Readconcern      string
	Params           string
	OperationTimeout time.Duration
}

// Item is Mongodb document wrapper.
type Item struct {
	Key   string      `bson:"_id"`
	Value interface{} `bson:"value"`
	Etag  string      `bson:"_etag"`
}

// NewMongoDB returns a new MongoDB state store.
func NewMongoDB(logger logger.Logger) documentstores.DocumentStore {
	ds := &MongoDB{
		features: []documentstores.Feature{},
		logger:   logger,
	}

	return ds
}

// Init establishes connection to the store based on the metadata.
func (m *MongoDB) Init(metadata documentstores.Metadata) error {
	meta, err := getMongoDBMetaData(metadata)
	if err != nil {
		return err
	}

	m.operationTimeout = meta.OperationTimeout

	client, err := getMongoDBClient(meta)
	if err != nil {
		return fmt.Errorf("error in creating mongodb client: %s", err)
	}

	if err = client.Ping(context.Background(), nil); err != nil {
		return fmt.Errorf("error in connecting to mongodb, host: %s error: %s", meta.Host, err)
	}

	m.client = client

	// get the write concern
	wc, err := getWriteConcernObject(meta.Writeconcern)
	if err != nil {
		return fmt.Errorf("error in getting write concern object: %s", err)
	}

	// get the read concern
	rc, err := getReadConcernObject(meta.Readconcern)
	if err != nil {
		return fmt.Errorf("error in getting read concern object: %s", err)
	}

	m.metadata = *meta
	opts := options.Collection().SetWriteConcern(wc).SetReadConcern(rc)
	collection := m.client.Database(meta.DatabaseName).Collection(meta.CollectionName, opts)

	m.collection = collection

	return nil
}

// Features returns the features available in this state store.
func (m *MongoDB) Features() []documentstores.Feature {
	return m.features
}

// Set saves state into MongoDB.
func (m *MongoDB) CreateDocument(ctx context.Context, req *documentstores.CreateDocumentRequest) (*documentstores.CreateDocumentResponse, error) {
	resp, err := m.setInternal(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Set saves state into MongoDB.
func (m *MongoDB) UpdateDocument(ctx context.Context, req *documentstores.UpdateDocumentRequest) (*documentstores.UpdateDocumentResponse, error) {
	resp := &documentstores.UpdateDocumentResponse{}
	return resp, nil
}

func (m *MongoDB) ReplaceDocument(ctx context.Context, req *documentstores.ReplaceDocumentRequest) (*documentstores.ReplaceDocumentResponse, error) {
	resp := &documentstores.ReplaceDocumentResponse{}
	return resp, nil
}

// Set saves state into MongoDB.
func (m *MongoDB) ListDocuments(ctx context.Context, req *documentstores.ListDocumentRequest) (*documentstores.ListDocumentResponse, error) {
	resp := &documentstores.ListDocumentResponse{}
	return resp, nil
}

func (m *MongoDB) Ping() error {
	if err := m.client.Ping(context.Background(), nil); err != nil {
		return fmt.Errorf("mongoDB store: error connecting to mongoDB at %s: %s", m.metadata.Host, err)
	}

	return nil
}

func (m *MongoDB) setInternal(ctx context.Context, req *documentstores.CreateDocumentRequest) (*documentstores.CreateDocumentResponse, error) {
	var v interface{}
	switch obj := req.Value.(type) {
	case []byte:
		v = string(obj)
	case string:
		v = fmt.Sprintf("%q", obj)
	default:
		v = req.Value
	}

	// create a document based on request key and value
	filter := bson.M{id: req.Key}
	if req.ETag != nil {
		filter[etag] = *req.ETag
	} else if req.Options.Concurrency == state.FirstWrite {
		filter[etag] = uuid.NewString()
	}

	update := bson.M{"$set": bson.M{id: req.Key, value: v, etag: uuid.NewString()}}
	_, err := m.collection.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))

	resp := &documentstores.CreateDocumentResponse{}
	return resp, err
}

// Get retrieves state from MongoDB with a key.
func (m *MongoDB) GetDocument(ctx context.Context, req *documentstores.GetDocumentRequest) (*documentstores.GetDocumentResponse, error) {
	var result Item

	filter := bson.M{id: req.Key}
	err := m.collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// Key not found, not an error.
			// To behave the same as other state stores in conf tests.
			return &documentstores.GetDocumentResponse{}, nil
		}

		return &documentstores.GetDocumentResponse{}, err
	}

	var data []byte
	switch obj := result.Value.(type) {
	case string:
		data = []byte(obj)
	case primitive.D:
		// Setting canonical to `false`.
		// See https://docs.mongodb.com/manual/reference/mongodb-extended-json/#bson-data-types-and-associated-representations
		// Having bson marshalled into Relaxed JSON instead of canonical JSON, this way type preservation is lost but
		// interoperability is preserved
		// See https://mongodb.github.io/swift-bson/docs/current/SwiftBSON/json-interop.html
		// A decimal value stored as BSON will be returned as {"d": 5.5} if canonical is set to false instead of
		// {"d": {"$numberDouble": 5.5}} when canonical JSON is returned.
		if data, err = bson.MarshalExtJSON(obj, false, true); err != nil {
			return &documentstores.GetDocumentResponse{}, err
		}
	case primitive.A:
		newobj := bson.D{{Key: value, Value: obj}}

		if data, err = bson.MarshalExtJSON(newobj, false, true); err != nil {
			return &documentstores.GetDocumentResponse{}, err
		}
		var input interface{}
		json.Unmarshal(data, &input)
		value := input.(map[string]interface{})[value]
		if data, err = json.Marshal(value); err != nil {
			return &documentstores.GetDocumentResponse{}, err
		}

	default:
		if data, err = json.Marshal(result.Value); err != nil {
			return &documentstores.GetDocumentResponse{}, err
		}
	}

	return &documentstores.GetDocumentResponse{
		Data: data,
		ETag: ptr.Of(result.Etag),
	}, nil
}

// Delete performs a delete operation.
func (m *MongoDB) DeleteDocument(ctx context.Context, req *documentstores.DeleteDocumentRequest) error {
	err := m.deleteInternal(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (m *MongoDB) deleteInternal(ctx context.Context, req *documentstores.DeleteDocumentRequest) error {
	filter := bson.M{id: req.Key}
	if req.ETag != nil {
		filter[etag] = *req.ETag
	}
	result, err := m.collection.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}

	if result.DeletedCount == 0 && req.ETag != nil {
		return errors.New("key or etag not found")
	}

	return nil
}

// Query executes a query against store.
func (m *MongoDB) QueryDocuments(ctx context.Context, req *documentstores.QueryDocumentRequest) (*documentstores.QueryDocumentResponse, error) {
	// TODO: implement this

	items := make([]documentstores.GetDocumentResponse, 0)

	return &documentstores.QueryDocumentResponse{
		Results:  items,
		Token:    "",
		Metadata: map[string]string{},
	}, nil
}

func getMongoURI(metadata *mongoDBMetadata) string {
	if len(metadata.Server) != 0 {
		if metadata.Username != "" && metadata.Password != "" {
			return fmt.Sprintf(connectionURIFormatWithSrvAndCredentials, metadata.Username, metadata.Password, metadata.Server, metadata.DatabaseName, metadata.Params)
		}

		return fmt.Sprintf(connectionURIFormatWithSrv, metadata.Server, metadata.Params)
	}

	if metadata.Username != "" && metadata.Password != "" {
		return fmt.Sprintf(connectionURIFormatWithAuthentication, metadata.Username, metadata.Password, metadata.Host, metadata.DatabaseName, metadata.Params)
	}

	return fmt.Sprintf(connectionURIFormat, metadata.Host, metadata.DatabaseName, metadata.Params)
}

func getMongoDBClient(metadata *mongoDBMetadata) (*mongo.Client, error) {
	uri := getMongoURI(metadata)

	// Set client options
	clientOptions := options.Client().ApplyURI(uri)

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), metadata.OperationTimeout)
	defer cancel()

	daprUserAgent := "dapr-" + logger.DaprVersion
	if clientOptions.AppName != nil {
		clientOptions.SetAppName(daprUserAgent + ":" + *clientOptions.AppName)
	} else {
		clientOptions.SetAppName(daprUserAgent)
	}

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func getMongoDBMetaData(meta documentstores.Metadata) (*mongoDBMetadata, error) {
	m := mongoDBMetadata{
		DatabaseName:     defaultDatabaseName,
		CollectionName:   defaultCollectionName,
		OperationTimeout: defaultTimeout,
	}

	decodeErr := metadata.DecodeMetadata(meta.Properties, &m)
	if decodeErr != nil {
		return nil, decodeErr
	}

	if len(m.Host) == 0 && len(m.Server) == 0 {
		return nil, errors.New("must set 'host' or 'server' fields in metadata")
	}

	if len(m.Host) != 0 && len(m.Server) != 0 {
		return nil, errors.New("'host' or 'server' fields are mutually exclusive")
	}

	var err error
	if val, ok := meta.Properties[operationTimeout]; ok && val != "" {
		m.OperationTimeout, err = time.ParseDuration(val)
		if err != nil {
			return nil, errors.New("incorrect operationTimeout field from metadata")
		}
	}

	return &m, nil
}

func getWriteConcernObject(cn string) (*writeconcern.WriteConcern, error) {
	var wc *writeconcern.WriteConcern
	if cn != "" {
		if cn == "majority" {
			wc = writeconcern.New(writeconcern.WMajority(), writeconcern.J(true), writeconcern.WTimeout(defaultTimeout))
		} else {
			w, err := strconv.Atoi(cn)
			wc = writeconcern.New(writeconcern.W(w), writeconcern.J(true), writeconcern.WTimeout(defaultTimeout))

			return wc, err
		}
	} else {
		wc = writeconcern.New(writeconcern.W(1), writeconcern.J(true), writeconcern.WTimeout(defaultTimeout))
	}

	return wc, nil
}

func getReadConcernObject(cn string) (*readconcern.ReadConcern, error) {
	switch cn {
	case "local":
		return readconcern.Local(), nil
	case "majority":
		return readconcern.Majority(), nil
	case "available":
		return readconcern.Available(), nil
	case "linearizable":
		return readconcern.Linearizable(), nil
	case "snapshot":
		return readconcern.Snapshot(), nil
	case "":
		return readconcern.Local(), nil
	}

	return nil, fmt.Errorf("readConcern %s not found", cn)
}

func (m *MongoDB) GetComponentMetadata() map[string]string {
	metadataStruct := mongoDBMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
