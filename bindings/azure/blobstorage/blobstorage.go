// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package blobstorage

import (
	"bytes"
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/google/uuid"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	// Used to reference the blob relative to the container.
	metadataKeyBlobName = "blobName"
	// A string value that identifies the portion of the list to be returned with the next list operation.
	// The operation returns a marker value within the response body if the list returned was not complete. The marker
	// value may then be used in a subsequent call to request the next set of list items.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs#uri-parameters
	metadataKeyMarker = "marker"
	// The number of blobs that will be returned in a list operation.
	metadataKeyNumber = "number"
	// Defines if the user defined metadata should be returned in the get operation.
	metadataKeyIncludeMetadata = "includeMetadata"
	// Defines the delete snapshots option for the delete operation.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/delete-blob#request-headers
	metadataKeyDeleteSnapshots = "deleteSnapshots"
	// HTTP headers to be associated with the blob.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#request-headers-all-blob-types
	metadataKeyContentType        = "contentType"
	metadataKeyContentMD5         = "contentMD5"
	metadataKeyContentEncoding    = "contentEncoding"
	metadataKeyContentLanguage    = "contentLanguage"
	metadataKeyContentDisposition = "contentDisposition"
	metadataKeyCacheControl       = "cacheControl"
	// Specifies the maximum number of HTTP GET requests that will be made while reading from a RetryReader. A value
	// of zero means that no additional HTTP GET requests will be made.
	defaultGetBlobRetryCount = 10
	// Specifies the maximum number of blobs to return, including all BlobPrefix elements. If the request does not
	// specify maxresults the server will return up to 5,000 items.
	// See: https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs#uri-parameters
	maxResults  = 5000
	endpointKey = "endpoint"
)

var ErrMissingBlobName = errors.New("blobName is a required attribute")

// AzureBlobStorage allows saving blobs to an Azure Blob Storage account.
type AzureBlobStorage struct {
	metadata *blobStorageMetadata
	// containerURL    azblob.ContainerURL
	containerClient azblob.ContainerClient

	logger logger.Logger
}

type blobStorageMetadata struct {
	StorageAccount    string                  `json:"storageAccount"`
	StorageAccessKey  string                  `json:"storageAccessKey"`
	Container         string                  `json:"container"`
	GetBlobRetryCount int                     `json:"getBlobRetryCount,string"`
	DecodeBase64      bool                    `json:"decodeBase64,string"`
	PublicAccessLevel azblob.PublicAccessType `json:"publicAccessLevel"`
}

type createResponse struct {
	BlobURL  string `json:"blobURL"`
	BlobName string `json:"blobName"`
}

type listInclude struct {
	Copy             bool `json:"copy"`
	Metadata         bool `json:"metadata"`
	Snapshots        bool `json:"snapshots"`
	UncommittedBlobs bool `json:"uncommittedBlobs"`
	Deleted          bool `json:"deleted"`
}

type listPayload struct {
	Marker     string      `json:"marker"`
	Prefix     string      `json:"prefix"`
	MaxResults int32       `json:"maxResults"`
	Include    listInclude `json:"include"`
}

// NewAzureBlobStorage returns a new Azure Blob Storage instance.
func NewAzureBlobStorage(logger logger.Logger) *AzureBlobStorage {
	return &AzureBlobStorage{logger: logger}
}

// Init performs metadata parsing.
func (a *AzureBlobStorage) Init(metadata bindings.Metadata) error {
	m, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	a.metadata = m

	userAgent := "dapr-" + logger.DaprVersion
	options := azblob.ClientOptions{
		Retry: policy.RetryOptions{
			MaxRetries: int32(a.metadata.GetBlobRetryCount),
		},
		Telemetry: policy.TelemetryOptions{
			ApplicationID: userAgent,
		},
	}

	settings, err := azauth.NewEnvironmentSettings("storage", metadata.Properties)
	if err != nil {
		return err
	}
	customEndpoint, ok := metadata.Properties[endpointKey]
	var URL *url.URL
	if ok && customEndpoint != "" {
		var parseErr error
		URL, parseErr = url.Parse(fmt.Sprintf("%s/%s/%s", customEndpoint, m.StorageAccount, m.Container))
		if parseErr != nil {
			return parseErr
		}
	} else {
		env := settings.AzureEnvironment
		URL, _ = url.Parse(fmt.Sprintf("https://%s.blob.%s/%s", m.StorageAccount, env.StorageEndpointSuffix, m.Container))
	}

	var containerClientErr error
	var containerClient azblob.ContainerClient
	// Try using shared key credentials first
	if m.StorageAccessKey != "" {
		credential, newSharedKeyErr := azblob.NewSharedKeyCredential(m.StorageAccount, m.StorageAccessKey)
		if err != nil {
			return fmt.Errorf("invalid credentials with error: %w", newSharedKeyErr)
		}
		containerClient, containerClientErr = azblob.NewContainerClientWithSharedKey(URL.String(), credential, &options)
		if containerClientErr != nil {
			return fmt.Errorf("Cannot init Blobstorage container client: %w", err)
		}
		a.containerClient = containerClient
	} else {
		// fallback to AAD
		credential, tokenErr := settings.GetTokenCredential()
		if err != nil {
			return fmt.Errorf("invalid credentials with error: %w", tokenErr)
		}
		containerClient, containerClientErr = azblob.NewContainerClient(URL.String(), credential, &options)

	}
	if containerClientErr != nil {
		return fmt.Errorf("Cannot init Blobstorage container client: %w", containerClientErr)
	}
	a.containerClient = containerClient

	ctx := context.Background()
	createContainerOptions := azblob.CreateContainerOptions{
		Access:   &m.PublicAccessLevel,
		Metadata: map[string]string{},
	}
	_, err = containerClient.Create(ctx, &createContainerOptions)
	// Don't return error, container might already exist
	a.logger.Debugf("error creating container: %w", err)
	a.containerClient = containerClient

	return nil
}

func (a *AzureBlobStorage) parseMetadata(metadata bindings.Metadata) (*blobStorageMetadata, error) {
	connInfo := metadata.Properties
	b, err := json.Marshal(connInfo)
	if err != nil {
		return nil, err
	}

	var m blobStorageMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	if m.GetBlobRetryCount == 0 {
		m.GetBlobRetryCount = defaultGetBlobRetryCount
	}

	// per the Dapr documentation "none" is a valid value
	if m.PublicAccessLevel == "none" {
		m.PublicAccessLevel = ""
	}
	if m.PublicAccessLevel != "" && !a.isValidPublicAccessType(m.PublicAccessLevel) {
		return nil, fmt.Errorf("invalid public access level: %s; allowed: %s",
			m.PublicAccessLevel, azblob.PossiblePublicAccessTypeValues())
	}

	return &m, nil
}

func (a *AzureBlobStorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.DeleteOperation,
		bindings.ListOperation,
	}
}

func (a *AzureBlobStorage) create(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blobHTTPHeaders azblob.BlobHTTPHeaders
	var blobName string
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blobName = val
		delete(req.Metadata, metadataKeyBlobName)
	} else {
		blobName = uuid.New().String()
	}

	if val, ok := req.Metadata[metadataKeyContentType]; ok && val != "" {
		blobHTTPHeaders.BlobContentType = &val
		delete(req.Metadata, metadataKeyContentType)
	}
	if val, ok := req.Metadata[metadataKeyContentMD5]; ok && val != "" {
		sDec, err := b64.StdEncoding.DecodeString(val)
		if err != nil || len(sDec) != 16 {
			return nil, fmt.Errorf("the MD5 value specified in Content MD5 is invalid, MD5 value must be 128 bits and base64 encoded")
		}
		blobHTTPHeaders.BlobContentMD5 = sDec
		delete(req.Metadata, metadataKeyContentMD5)
	}
	if val, ok := req.Metadata[metadataKeyContentEncoding]; ok && val != "" {
		blobHTTPHeaders.BlobContentEncoding = &val
		delete(req.Metadata, metadataKeyContentEncoding)
	}
	if val, ok := req.Metadata[metadataKeyContentLanguage]; ok && val != "" {
		blobHTTPHeaders.BlobContentLanguage = &val
		delete(req.Metadata, metadataKeyContentLanguage)
	}
	if val, ok := req.Metadata[metadataKeyContentDisposition]; ok && val != "" {
		blobHTTPHeaders.BlobContentDisposition = &val
		delete(req.Metadata, metadataKeyContentDisposition)
	}
	if val, ok := req.Metadata[metadataKeyCacheControl]; ok && val != "" {
		blobHTTPHeaders.BlobCacheControl = &val
		delete(req.Metadata, metadataKeyCacheControl)
	}

	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	if a.metadata.DecodeBase64 {
		decoded, decodeError := b64.StdEncoding.DecodeString(string(req.Data))
		if decodeError != nil {
			return nil, decodeError
		}
		req.Data = decoded
	}

	blockBlobClient := a.containerClient.NewBlockBlobClient(blobName)
	uploadOptions := azblob.HighLevelUploadToBlockBlobOption{
		HTTPHeaders: &blobHTTPHeaders,
		Metadata:    req.Metadata,
		Parallelism: 16,
	}

	_, err = blockBlobClient.UploadBufferToBlockBlob(context.Background(), req.Data, uploadOptions)

	if err != nil {
		return nil, fmt.Errorf("error uploading az blob: %w", err)
	}

	resp := createResponse{
		BlobURL: blockBlobClient.URL(),
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("error marshalling create response for azure blob: %w", err)
	}

	createResponseMetadata := map[string]string{
		"blobName": blobName,
	}

	return &bindings.InvokeResponse{
		Data:     b,
		Metadata: createResponseMetadata,
	}, nil
}

func (a *AzureBlobStorage) get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blockBlobClient azblob.BlockBlobClient
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blockBlobClient = a.containerClient.NewBlockBlobClient(val)
	} else {
		return nil, ErrMissingBlobName
	}

	ctx := context.TODO()

	get, err := blockBlobClient.Download(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("error starting download of az blob: %w", err)
	}

	// Open a buffer, reader, and then download!
	downloadedData := &bytes.Buffer{}
	reader := get.Body(azblob.RetryReaderOptions{
		MaxRetryRequests: a.metadata.GetBlobRetryCount,
	})
	_, err = downloadedData.ReadFrom(reader)
	if err != nil {
		return nil, fmt.Errorf("error downloading az blob: %w", err)
	}

	var metadata map[string]string
	fetchMetadata, err := req.GetMetadataAsBool(metadataKeyIncludeMetadata)
	if err != nil {
		return nil, fmt.Errorf("error parsing metadata: %w", err)
	}

	if fetchMetadata {
		props, err := blockBlobClient.GetProperties(ctx, &azblob.GetBlobPropertiesOptions{})
		if err != nil {
			return nil, fmt.Errorf("error reading blob metadata: %w", err)
		}

		metadata = props.Metadata
	}

	return &bindings.InvokeResponse{
		Data:     downloadedData.Bytes(),
		Metadata: metadata,
	}, nil
}

func (a *AzureBlobStorage) delete(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var blockBlobClient azblob.BlockBlobClient
	if val, ok := req.Metadata[metadataKeyBlobName]; ok && val != "" {
		blockBlobClient = a.containerClient.NewBlockBlobClient(val)
	} else {
		return nil, ErrMissingBlobName
	}

	var deleteSnapshotsOptions azblob.DeleteSnapshotsOptionType
	if val, ok := req.Metadata[metadataKeyDeleteSnapshots]; ok && val != "" {
		deleteSnapshotsOptions = azblob.DeleteSnapshotsOptionType(val)
		if !a.isValidDeleteSnapshotsOptionType(deleteSnapshotsOptions) {
			return nil, fmt.Errorf("invalid delete snapshot option type: %s; allowed: %s",
				deleteSnapshotsOptions, azblob.PossibleDeleteSnapshotsOptionTypeValues())
		}
	}

	deleteOptions := azblob.DeleteBlobOptions{
		DeleteSnapshots:      &deleteSnapshotsOptions,
		BlobAccessConditions: &azblob.BlobAccessConditions{},
	}

	_, err := blockBlobClient.Delete(context.Background(), &deleteOptions)

	return nil, err
}

func (a *AzureBlobStorage) list(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	options := azblob.ContainerListBlobFlatSegmentOptions{}

	hasPayload := false
	var payload listPayload
	if req.Data != nil {
		err := json.Unmarshal(req.Data, &payload)
		if err != nil {
			return nil, err
		}
		hasPayload = true
	}

	if hasPayload {
		var includeItems []azblob.ListBlobsIncludeItem
		if payload.Include.Copy {
			includeItems = append(includeItems, azblob.ListBlobsIncludeItemCopy)
		}
		if payload.Include.Metadata {
			includeItems = append(includeItems, azblob.ListBlobsIncludeItemMetadata)
		}
		if payload.Include.Snapshots {
			includeItems = append(includeItems, azblob.ListBlobsIncludeItemSnapshots)
		}
		if payload.Include.UncommittedBlobs {
			includeItems = append(includeItems, azblob.ListBlobsIncludeItemUncommittedblobs)
		}
		if payload.Include.Deleted {
			includeItems = append(includeItems, azblob.ListBlobsIncludeItemDeleted)
		}

		options.Include = append(options.Include, includeItems...)
	}

	if hasPayload && payload.MaxResults != int32(0) {
		options.Maxresults = &payload.MaxResults
	} else {
		maxResults := int32(maxResults)
		options.Maxresults = &maxResults
	}

	if hasPayload && payload.Prefix != "" {
		options.Prefix = &payload.Prefix
	}

	var initialMarker string
	if hasPayload && payload.Marker != "" {
		initialMarker = payload.Marker
	} else {
		initialMarker = ""
	}
	options.Marker = &initialMarker

	var blobs []*azblob.BlobItemInternal
	metadata := map[string]string{}
	ctx := context.Background()
	var listBlobPager *azblob.ContainerListBlobFlatSegmentPager = a.containerClient.ListBlobsFlat(&options)
	for listBlobPager.NextPage(ctx) {
		currentPage := listBlobPager.PageResponse()
		blobs = append(blobs, currentPage.ContainerListBlobFlatSegmentResult.Segment.BlobItems...)

		numBlobs := len(blobs)
		metadata[metadataKeyMarker] = *currentPage.Marker
		metadata[metadataKeyNumber] = strconv.FormatInt(int64(numBlobs), 10)

		if *options.Maxresults-maxResults > 0 {
			*options.Maxresults -= maxResults
		} else {
			break
		}

	}

	jsonResponse, err := json.Marshal(blobs)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal blobs to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data:     jsonResponse,
		Metadata: metadata,
	}, nil
}

func (a *AzureBlobStorage) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		return a.create(req)
	case bindings.GetOperation:
		return a.get(req)
	case bindings.DeleteOperation:
		return a.delete(req)
	case bindings.ListOperation:
		return a.list(req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

func (a *AzureBlobStorage) isValidPublicAccessType(accessType azblob.PublicAccessType) bool {
	validTypes := azblob.PossiblePublicAccessTypeValues()
	for _, item := range validTypes {
		if item == accessType {
			return true
		}
	}

	return false
}

func (a *AzureBlobStorage) isValidDeleteSnapshotsOptionType(accessType azblob.DeleteSnapshotsOptionType) bool {
	validTypes := azblob.PossibleDeleteSnapshotsOptionTypeValues()
	for _, item := range validTypes {
		if item == accessType {
			return true
		}
	}

	return false
}
