package bucket

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// defaultMultipartAbove is the largest object written in one request.
	defaultMultipartAbove = 256 << 20
	// defaultPartSize is the smallest part a larger object is written in.
	defaultPartSize = 64 << 20
	// maxParts is the most parts S3 assembles one object from.
	maxParts = 10000
	// maxListKeys is the most objects S3 lists in one page.
	maxListKeys = 1000
	// maxListResponse bounds a page of keys, each up to 1 KiB and escaped.
	maxListResponse = 16 << 20

	metaHeaderPrefix = "X-Amz-Meta-"
)

// emptyPayloadSHA256 is the payload hash of a request without a body.
var emptyPayloadSHA256 = hex.EncodeToString(sha256.New().Sum(nil))

// rgwObjects is a session on one bucket's objects. It signs as the bucket's
// owner with a key Close drops. Two sessions on one bucket share that key,
// and the first to close drops it: the other's next request fails and its
// caller retries with a new session.
type rgwObjects struct {
	b      *rgw
	uid    string
	bucket string
	signer s3Signer
}

var _ Objects = (*rgwObjects)(nil)

func (b *rgw) Objects(ctx context.Context, ref Ref) (Objects, error) {
	user, err := b.user(ctx, ref)
	if err != nil {
		return nil, err
	}

	exists, err := b.bucketOwnedBy(ctx, ref)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: bucket %s", ErrNotFound, ref.Bucket)
	}

	signer, err := b.ownerSigner(ctx, user)
	if err != nil {
		return nil, err
	}

	return &rgwObjects{b: b, uid: ref.UserID, bucket: ref.Bucket, signer: signer}, nil
}

func (o *rgwObjects) Close() error {
	// the request the session served may be over; its key goes regardless
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return o.b.dropOwnerKeys(ctx, o.uid)
}

type listBucketResult struct {
	IsTruncated  bool   `xml:"IsTruncated"`
	EncodingType string `xml:"EncodingType"`
	Contents     []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
}

func (o *rgwObjects) List(ctx context.Context, after string, limit int) ([]ObjectInfo, bool, error) {
	if limit <= 0 || limit > maxListKeys {
		limit = maxListKeys
	}

	// keys come back escaped: XML cannot carry every byte a key may hold
	query := url.Values{
		"list-type":     {"2"},
		"encoding-type": {"url"},
		"max-keys":      {strconv.Itoa(limit)},
	}
	if after != "" {
		query.Set("start-after", after)
	}

	resp, err := o.do(ctx, s3Call{method: http.MethodGet, query: query})
	if err != nil {
		return nil, false, fmt.Errorf("list objects of %s: %w", o.bucket, err)
	}
	defer drain(resp)

	if resp.StatusCode != http.StatusOK {
		return nil, false, s3Error("list objects of "+o.bucket, resp)
	}

	var result listBucketResult
	if err := xml.NewDecoder(io.LimitReader(resp.Body, maxListResponse)).Decode(&result); err != nil {
		return nil, false, fmt.Errorf("list objects of %s: %w", o.bucket, err)
	}

	objects := make([]ObjectInfo, 0, len(result.Contents))
	for _, entry := range result.Contents {
		key := entry.Key
		if result.EncodingType == "url" {
			if key, err = url.QueryUnescape(entry.Key); err != nil {
				return nil, false, fmt.Errorf("list objects of %s: %w", o.bucket, err)
			}
		}

		objects = append(objects, ObjectInfo{Key: key, Size: entry.Size})
	}

	return objects, result.IsTruncated, nil
}

func (o *rgwObjects) Get(ctx context.Context, key string) (ObjectInfo, io.ReadCloser, error) {
	resp, err := o.do(ctx, s3Call{method: http.MethodGet, key: key})
	if err != nil {
		return ObjectInfo{}, nil, fmt.Errorf("get object of %s: %w", o.bucket, err)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound:
		drain(resp)
		return ObjectInfo{}, nil, ErrObjectNotFound
	case resp.StatusCode != http.StatusOK:
		defer drain(resp)
		return ObjectInfo{}, nil, s3Error("get object of "+o.bucket, resp)
	case resp.ContentLength < 0:
		drain(resp)
		return ObjectInfo{}, nil, fmt.Errorf("get object of %s: no content length", o.bucket)
	}

	info := ObjectInfo{
		Key:                key,
		Size:               resp.ContentLength,
		ContentType:        resp.Header.Get("Content-Type"),
		ContentEncoding:    resp.Header.Get("Content-Encoding"),
		ContentDisposition: resp.Header.Get("Content-Disposition"),
		ContentLanguage:    resp.Header.Get("Content-Language"),
		CacheControl:       resp.Header.Get("Cache-Control"),
		Expires:            resp.Header.Get("Expires"),
	}

	for name, values := range resp.Header {
		if meta, ok := strings.CutPrefix(name, metaHeaderPrefix); ok && len(values) > 0 {
			if info.Metadata == nil {
				info.Metadata = make(map[string]string)
			}
			info.Metadata[strings.ToLower(meta)] = values[0]
		}
	}

	return info, resp.Body, nil
}

func (o *rgwObjects) Put(ctx context.Context, info ObjectInfo, body io.ReaderAt, sum []byte) error {
	if info.Size < 0 {
		return fmt.Errorf("put object of %s: negative size %d", o.bucket, info.Size)
	}

	if info.Size > o.b.multipartAbove {
		return o.putParts(ctx, info, body, sum)
	}

	resp, err := o.do(ctx, s3Call{
		method: http.MethodPut,
		key:    info.Key,
		header: objectHeaders(info),
		body:   io.NewSectionReader(body, 0, info.Size),
		length: info.Size,
		sha256: hex.EncodeToString(sum),
	})
	if err != nil {
		return fmt.Errorf("put object of %s: %w", o.bucket, err)
	}
	defer drain(resp)

	if resp.StatusCode != http.StatusOK {
		return s3Error("put object of "+o.bucket, resp)
	}

	return nil
}

type completedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUpload struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload"`
	Parts   []completedPart `xml:"Part"`
}

// putParts writes a large object in parts, each checked by the S3 server
// against its own sha256. The whole object is checked against sum before
// the parts are put together; an upload that fails is aborted, so its
// parts take no space.
func (o *rgwObjects) putParts(ctx context.Context, info ObjectInfo, body io.ReaderAt, sum []byte) (err error) {
	partSize := max(o.b.partSize, (info.Size+maxParts-1)/maxParts)

	uploadID, err := o.createUpload(ctx, info)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = errors.Join(err, o.abortUpload(context.WithoutCancel(ctx), info.Key, uploadID))
		}
	}()

	whole := sha256.New()
	parts := make([]completedPart, 0, (info.Size+partSize-1)/partSize)

	for number, offset := 1, int64(0); offset < info.Size; number, offset = number+1, offset+partSize {
		size := min(partSize, info.Size-offset)

		digest := sha256.New()
		if _, err := io.Copy(io.MultiWriter(digest, whole), io.NewSectionReader(body, offset, size)); err != nil {
			return fmt.Errorf("put object of %s: %w", o.bucket, err)
		}

		etag, err := o.uploadPart(ctx, info.Key, uploadID, number, io.NewSectionReader(body, offset, size), size, digest.Sum(nil))
		if err != nil {
			return err
		}

		parts = append(parts, completedPart{PartNumber: number, ETag: etag})
	}

	if !bytes.Equal(whole.Sum(nil), sum) {
		return fmt.Errorf("put object of %s: the content does not match its sha256", o.bucket)
	}

	return o.completeUpload(ctx, info.Key, uploadID, parts)
}

func (o *rgwObjects) createUpload(ctx context.Context, info ObjectInfo) (string, error) {
	resp, err := o.do(ctx, s3Call{method: http.MethodPost, key: info.Key, query: url.Values{"uploads": {""}}, header: objectHeaders(info)})
	if err != nil {
		return "", fmt.Errorf("start upload to %s: %w", o.bucket, err)
	}
	defer drain(resp)

	if resp.StatusCode != http.StatusOK {
		return "", s3Error("start upload to "+o.bucket, resp)
	}

	var result struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&result); err != nil || result.UploadID == "" {
		return "", fmt.Errorf("start upload to %s: no upload id: %v", o.bucket, err)
	}

	return result.UploadID, nil
}

func (o *rgwObjects) uploadPart(ctx context.Context, key, uploadID string, number int, part io.Reader, size int64, sum []byte) (string, error) {
	resp, err := o.do(ctx, s3Call{
		method: http.MethodPut,
		key:    key,
		query:  url.Values{"partNumber": {strconv.Itoa(number)}, "uploadId": {uploadID}},
		body:   part,
		length: size,
		sha256: hex.EncodeToString(sum),
	})
	if err != nil {
		return "", fmt.Errorf("upload part %d to %s: %w", number, o.bucket, err)
	}
	defer drain(resp)

	if resp.StatusCode != http.StatusOK {
		return "", s3Error(fmt.Sprintf("upload part %d to %s", number, o.bucket), resp)
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("upload part %d to %s: no etag", number, o.bucket)
	}

	return etag, nil
}

func (o *rgwObjects) completeUpload(ctx context.Context, key, uploadID string, parts []completedPart) error {
	body, err := xml.Marshal(completeMultipartUpload{Parts: parts})
	if err != nil {
		return err
	}

	sum := sha256.Sum256(body)

	resp, err := o.do(ctx, s3Call{
		method: http.MethodPost,
		key:    key,
		query:  url.Values{"uploadId": {uploadID}},
		body:   bytes.NewReader(body),
		length: int64(len(body)),
		sha256: hex.EncodeToString(sum[:]),
	})
	if err != nil {
		return fmt.Errorf("complete upload to %s: %w", o.bucket, err)
	}
	defer drain(resp)

	// S3 can answer 200 and still fail, with the error in the body
	reply, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("complete upload to %s: %w", o.bucket, err)
	}

	if resp.StatusCode != http.StatusOK || bytes.Contains(reply, []byte("<Error>")) {
		return fmt.Errorf("complete upload to %s: %d: %s", o.bucket, resp.StatusCode, bytes.TrimSpace(reply))
	}

	return nil
}

func (o *rgwObjects) abortUpload(ctx context.Context, key, uploadID string) error {
	resp, err := o.do(ctx, s3Call{method: http.MethodDelete, key: key, query: url.Values{"uploadId": {uploadID}}})
	if err != nil {
		return fmt.Errorf("abort upload to %s: %w", o.bucket, err)
	}
	defer drain(resp)

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return s3Error("abort upload to "+o.bucket, resp)
	}

	return nil
}

func (o *rgwObjects) Delete(ctx context.Context, key string) error {
	resp, err := o.do(ctx, s3Call{method: http.MethodDelete, key: key})
	if err != nil {
		return fmt.Errorf("delete object of %s: %w", o.bucket, err)
	}
	defer drain(resp)

	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusNotFound:
		return nil
	default:
		return s3Error("delete object of "+o.bucket, resp)
	}
}

// objectHeaders are the headers an object is written with, so it is served
// as it was before.
func objectHeaders(info ObjectInfo) http.Header {
	header := make(http.Header)

	for name, value := range map[string]string{
		"Content-Type":        info.ContentType,
		"Content-Encoding":    info.ContentEncoding,
		"Content-Disposition": info.ContentDisposition,
		"Content-Language":    info.ContentLanguage,
		"Cache-Control":       info.CacheControl,
		"Expires":             info.Expires,
	} {
		if value != "" {
			header.Set(name, value)
		}
	}

	for name, value := range info.Metadata {
		header.Set(metaHeaderPrefix+name, value)
	}

	return header
}

type s3Call struct {
	method string
	// key is the object; empty addresses the bucket itself
	key    string
	query  url.Values
	header http.Header
	body   io.Reader
	length int64
	// sha256 is the hex payload hash; empty for a request without a body
	sha256 string
}

// do makes a path-style S3 request to the in-cluster endpoint, signed as
// the session's owner.
func (o *rgwObjects) do(ctx context.Context, call s3Call) (*http.Response, error) {
	u, err := url.Parse(o.b.endpoint)
	if err != nil {
		return nil, err
	}

	path := strings.TrimRight(u.Path, "/") + "/" + o.bucket
	raw := strings.TrimRight(u.EscapedPath(), "/") + "/" + o.bucket
	if call.key != "" {
		path += "/" + call.key
		raw += "/" + escapeKey(call.key)
	}
	u.Path, u.RawPath = path, raw
	u.RawQuery = strings.ReplaceAll(call.query.Encode(), "+", "%20")

	body := call.body
	if body == nil {
		body = http.NoBody
	}

	req, err := http.NewRequestWithContext(ctx, call.method, o.b.endpoint, body)
	if err != nil {
		return nil, err
	}
	req.URL = u
	req.ContentLength = call.length

	hash := call.sha256
	if hash == "" {
		hash = emptyPayloadSHA256
	}
	req.Header.Set("X-Amz-Content-Sha256", hash)
	for name, values := range call.header {
		req.Header[name] = values
	}

	if err := o.signer.sign(ctx, req, hash); err != nil {
		return nil, err
	}

	return o.b.data.Do(req)
}

// escapeKey percent-encodes an object key the way S3 signs it: every byte
// but the unreserved characters and the slashes between its segments.
func escapeKey(key string) string {
	var b strings.Builder

	for i := 0; i < len(key); i++ {
		c := key[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || strings.IndexByte("-_.~/", c) >= 0 {
			b.WriteByte(c)
			continue
		}

		fmt.Fprintf(&b, "%%%02X", c)
	}

	return b.String()
}

// s3Error reads the S3 error a response carries.
func s3Error(what string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("%s: %d: %s", what, resp.StatusCode, bytes.TrimSpace(body))
}

// drain lets the connection be reused.
func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
}
