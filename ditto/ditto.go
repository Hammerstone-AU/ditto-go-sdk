// Package ditto provides a thin client for interacting with a Ditto Edge HTTP
// API as well as helpers to manage the lifecycle of a Ditto Edge container via
// Docker or Docker Compose. The client issues parameterized DQL for safety and
// compatibility across Ditto versions.
package ditto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

/*

   Ditto Edge: https://www.ditto.live/
   Ditto DQL:  https://docs.ditto.live/docs/dql/introduction
   Ditto HTTP API: https://docs.ditto.live/docs/http-api/introduction
   Ditto Docker Image: https://hub.docker.com/r/dittoedge/server

   This package is designed to be compatible with Ditto Edge v0.6.x and later.

   FUNCTIONS:
   - NewService(baseURL, appID string) *service
       Creates a new Ditto service client targeting the specified HTTP API base URL
       and application (database) ID. The returned service uses a default HTTP client
       with a 30-second timeout. To enable Docker/Compose management, call WithDocker.
   - (s *service) WithDocker(docker DockerRunner, opts DockerOptions) *service
       Attaches a DockerRunner to the service for container lifecycle management.
       The provided DockerOptions are stored for use during InitDB and Close.
   - (s *service) InitDB(ctx context.Context) error
       Ensures the Ditto Edge container is running. If a DockerRunner is attached,
       it checks for the image (loading from tar if necessary), starts an existing
       container if exited, or runs a new one if not found. Marks the container as
       started by this process so Close can stop it. If no DockerRunner is attached,
       this is a no-op.
   - (s *service) Close(ctx context.Context) error
       Attempts to stop the Ditto container if a DockerRunner is attached. Safe to
       call multiple times; ignores errors on shutdown.
   - (s *service) Status(ctx context.Context) (map[string]any, error)
       Returns diagnostic information including Docker (Compose) container status
       and a Ditto HTTP probe result using a lightweight SELECT query.
   - (s *service) CreateDocument(ctx context.Context, collection string, doc map[string]any) (any, error)
       Inserts a single JSON document into the specified collection using a
       parameterized INSERT DQL statement.
   - (s *service) GetRecord(ctx context.Context, collection, id string) (any, error)
       Fetches a single record by its _id using a parameterized SELECT query.
   - (s *service) GetRecords(ctx context.Context, collection string, limit int, sortBy, sortOrder string) (any, error)
       Returns documents from the specified collection with optional LIMIT and ORDER BY.
   - (s *service) UpdateRecord(ctx context.Context, collection, id string, patch map[string]any) (any, error)
       Applies a JSON patch (field map) to a record identified by _id using
       parameterized SET clauses in an UPDATE DQL statement.
   - (s *service) DeleteRecord(ctx context.Context, collection, id string) (any, error)
       Removes a single record by its _id using a parameterized EVICT DQL statement.
   - (s *service) DeleteAllRecords(ctx context.Context, collection string) (any, error)
       Removes all documents in a collection using an EVICT statement with a
       LIKE pattern that matches all identifiers.
   - (s *service) LatestRecord(ctx context.Context, collection, sortBy string) (any, error)
       Returns the most recent record in a collection according to the provided
       field (descending order), limited to a single result.
   - (s *service) Search(ctx context.Context, collection string, filters map[string]string, limit int, sortBy, sortOrder string) (any, error)
       Builds a simple exact-match WHERE clause from the provided filters and
       applies optional LIMIT and ORDER BY.
   - BuildSelect(collection string, filters map[string]string, limit int, sortBy, sortOrder string) string
       Constructs a DQL SELECT statement for the specified collection with optional
       exact-match filters, limit, and ordering.
   - BuildInsert(collection string, doc map[string]any) (string, map[string]any, error)
       Constructs an INSERT DQL statement with a parameterized document (:doc).
   - BuildUpdate(collection, id string, patch map[string]any) (string, map[string]any, error)
       Constructs an UPDATE DQL statement with parameterized SET clauses and a
       bound :id for the target record.
   - escapeIdent(s string) string
       Performs minimal identifier sanitization suitable for DQL by removing
       backticks and replacing spaces with underscores.
   - escapeString(s string) string
       Escapes double quotes in a string literal.
   - NewDockerRunnerDefault() DockerRunner
       Returns a DockerRunner that manages containers using plain `docker` CLI
       commands (no Compose integration).
   - NewComposeRunnerDefault() DockerRunner
       Returns a DockerRunner that manages containers using `docker compose`
       commands.
   - (d *dockerRunnerDefault) EnsureImageLoaded(ctx context.Context, imageName, tarPath string) error
       Checks for the specified Docker image locally and loads it from a tarball
       if it is missing. If tarPath is empty, it assumes the image is available
       or will be pulled by other means.
   - (d *dockerRunnerDefault) ContainerStatus(ctx context.Context, name string) (string, error)
       Returns a coarse status for the specified container: running, exited,
       not-found, or a raw status string from `docker ps`.
   - (d *dockerRunnerDefault) RunContainer(ctx context.Context, opts DockerOptions) error
       Starts a new Ditto Edge container using `docker run`, wiring the config
       and data mounts and exposing the HTTP API port.
   - (d *dockerRunnerDefault) StartContainer(ctx context.Context, name string) error
       Starts a previously created container using `docker start`.
   - (d *dockerRunnerDefault) StopContainer(ctx context.Context, name string) error
       Stops a running container using `docker stop`.
   - (d *composeRunnerDefault) EnsureImageLoaded(ctx context.Context, imageName, tarPath string) error
       Mirrors the behavior of dockerRunnerDefault for parity.
   - (d *composeRunnerDefault) ContainerStatus(ctx context.Context, name string) (string, error)
       Reports the status of the specified container using `docker ps`.
   - (d *composeRunnerDefault) RunContainer(ctx context.Context, opts DockerOptions) error
       Brings the compose service up with `docker compose up -d`.
   - (d *composeRunnerDefault) StartContainer(ctx context.Context, name string) error
       Starts a stopped compose service using `docker compose start`.
   - (d *composeRunnerDefault) StopContainer(ctx context.Context, name string) error
       Stops the compose service and then best-effort stops/removes any lingering
       container by name.
   - runCmd(ctx context.Context, name string, args ...string) error
       Executes a CLI command and returns a formatted error including stdout/stderr
       when the command fails.
   - DockerRunner interface
       Abstracts container lifecycle operations so the service can run with either
       plain Docker or Docker Compose backends.
   - DockerOptions struct
       Collects parameters for starting a Ditto Edge container, including optional
       Docker Compose settings.
   - Service interface
       Defines the operations the HTTP handlers expect. Implementations are
       responsible for connecting to Ditto's HTTP API and translating these methods
       into appropriate DQL statements (parameterized when mutating state).
   - service struct
       Implements Service using a standard net/http client and optional
       Docker/Compose integration to manage the Ditto Edge container.
   - dittoModuleCode constant
       Module code for logging (kept for reference if per-module numeric codes
       are desired).

   Basic usage:
       import "path/to/ditto"
       s := ditto.NewService("http://localhost:8090", "myapp")
       // Optionally enable Docker management
       docker := ditto.NewDockerRunnerDefault() // or NewComposeRunnerDefault()
       s.WithDocker(docker, ditto.DockerOptions{
	   ContainerName: "ditto-edge",
	   ImageName:     "dittoedge/server:latest",
	   ImageTarPath:  "/path/to/dittoedge-server-latest.tar", // optional
	   ConfigPath:    "/path/to/config.yaml",
	   DataPath:      "/path/to/data",
	   // Optional compose settings:
	   // ComposeFile:   "/path/to/docker-compose.yml",
	   // ComposeService: "ditto-edge-server",
       })
       err := s.InitDB(context.Background())
       defer s.Close(context.Background())
       if err != nil { log.Fatal(err) }
       status, err := s.Status(context.Background())
       if err != nil { log.Fatal(err) }
       fmt.Printf("Ditto status: %+v\n", status)
       doc := map[string]any{"name": "Alice", "age": 30}
       res, err := s.CreateDocument(context.Background(), "users", doc)
       if err != nil { log.Fatal(err) }
       fmt.Printf("Insert result: %+v\n", res)




*/

// Module code for logging (kept for reference if per-module numeric codes are desired)
const dittoModuleCode = 300

// Service defines the operations the HTTP handlers expect. Implementations are
// responsible for connecting to Ditto's HTTP API and translating these methods
// into appropriate DQL statements (parameterized when mutating state).
type Service interface {
	InitDB(ctx context.Context) error
	Close(ctx context.Context) error
	Status(ctx context.Context) (map[string]any, error)

	CreateDocument(ctx context.Context, collection string, doc map[string]any) (any, error)
	GetRecord(ctx context.Context, collection, id string) (any, error)
	GetRecords(
		ctx context.Context,
		collection string,
		limit int,
		sortBy, sortOrder string,
	) (any, error)
	UpdateRecord(ctx context.Context, collection, id string, patch map[string]any) (any, error)
	DeleteRecord(ctx context.Context, collection, id string) (any, error)
	DeleteAllRecords(ctx context.Context, collection string) (any, error)
	LatestRecord(ctx context.Context, collection, sortBy string) (any, error)
	Search(
		ctx context.Context,
		collection string,
		filters map[string]string,
		limit int,
		sortBy, sortOrder string,
	) (any, error)
}

// Implementation -------------------------------------------------------------

// service implements Service using a standard net/http client and optional
// Docker/Compose integration to manage the Ditto Edge container.
type service struct {
	BaseURL       string
	AppID         string
	HTTP          *http.Client
	docker        DockerRunner
	dockerOpts    DockerOptions
	startedDocker bool
}

// NewService constructs a new Ditto service targeting the given Ditto HTTP API
// base URL and application (DB) ID. A default HTTP client with a reasonable
// timeout is installed. To enable container management, call WithDocker.
func NewService(baseURL, appID string) *service {
	return &service{
		BaseURL: baseURL,
		AppID:   appID,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
}

// WithDocker attaches a DockerRunner to the service so InitDB/Close can manage
// the Ditto container. The provided options are stored for subsequent calls.
func (s *service) WithDocker(docker DockerRunner, opts DockerOptions) *service {
	// Attach Docker runner and options
	// If docker is nil, container management is disabled
	// (InitDB becomes a no-op)
	s.docker = docker
	s.dockerOpts = opts
	return s
}

// InitDB ensures the Ditto Edge container is ready. Behavior:
// - If a DockerRunner is not attached, this is a no-op.
// - Loads the image from a tar if missing; otherwise relies on existing image.
// - Starts an existing container if exited; otherwise runs a new one.
// - Marks the container as started by this process so Close can stop it.
func (s *service) InitDB(ctx context.Context) error {
	// No-op if no DockerRunner attached
	if s.docker == nil {
		// Docker disabled
		return nil
	}
	// Ensure image is present and container is running
	if err := s.docker.EnsureImageLoaded(ctx, s.dockerOpts.ImageName, s.dockerOpts.ImageTarPath); err != nil {
		return fmt.Errorf("ensure image: %w", err)
	}

	// Check container status
	// Possible results: running, exited, not-found
	status, err := s.docker.ContainerStatus(ctx, s.dockerOpts.ContainerName)
	if err != nil {
		return fmt.Errorf("container status: %w", err)
	}

	// Act based on status
	if status == "running" {
		return nil
	}

	// Exited, start it
    if status == "exited" {
        // Recreate via RunContainer to pick up volume/mount changes in compose.
        if err := s.docker.RunContainer(ctx, s.dockerOpts); err != nil {
            return fmt.Errorf("start container: %w", err)
        }
        return nil
    }
	// Not found, run new
	if err := s.docker.RunContainer(ctx, s.dockerOpts); err != nil {
		return fmt.Errorf("run container: %w", err)
	}

	// Mark as started by this process
	s.startedDocker = true
	return nil
}

// Close attempts to stop the Ditto container using the attached DockerRunner.
// This method is safe to call multiple times and ignores errors on shutdown.
func (s *service) Close(ctx context.Context) error {
	// No-op if no DockerRunner attached or if we didn't start the container
	if s.docker != nil {
		_ = s.docker.StopContainer(ctx, s.dockerOpts.ContainerName)
	}
	return nil
}

// Status returns diagnostic information including Docker (Compose) container
// status and a Ditto HTTP probe result using a lightweight SELECT.
func (s *service) Status(ctx context.Context) (map[string]any, error) {
	// Base info
	// Docker status if enabled, plus HTTP probe
	// res stands for result
	// s.BaseURL stand for Ditto HTTP API base URL
	// s.AppID stands for Ditto application (database) ID
	// If Docker is enabled, get container status; otherwise note disabled
	// Probe Ditto HTTP server with a lightweight query
	// Use a simple SELECT to verify connectivity
	// Return a map with baseURL, appID, docker status, and http status
	// If Docker status check fails, include the error message
	// If HTTP probe fails, include the error message
	// Return the result map and any error encountered
	res := map[string]any{"baseURL": s.BaseURL, "appID": s.AppID}
	if s.docker != nil {
		st, err := s.docker.ContainerStatus(ctx, s.dockerOpts.ContainerName)
		if err != nil {
			res["dockerError"] = err.Error()
		} else {
			res["docker"] = st
		}
	} else {
		res["docker"] = "disabled"
	}
	// Probe Ditto HTTP server (use FROM to satisfy DQL)
	url := fmt.Sprintf("%s/%s/execute", strings.TrimRight(s.BaseURL, "/"), s.AppID)
	body := map[string]string{"query": "SELECT * FROM chat LIMIT 1"}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.HTTP.Do(req)
	if err != nil {
		res["http"] = "unreachable"
		res["httpError"] = err.Error()
		return res, nil
	}
	defer resp.Body.Close()
	res["http"] = resp.Status
	return res, nil
}

// CreateDocument inserts a single JSON document into the specified collection.
// The DQL uses a bound parameter (:doc) to pass the document as query_args.
func (s *service) CreateDocument(
	// ctx stands for context
	// collection stands for Ditto collection name
	// doc stands for document to insert (map[string]any)
	ctx context.Context,
	collection string,
	doc map[string]any,
) (any, error) {
	// Build parameterized INSERT DQL
	// q stands for query
	// args stands for query arguments
	// err stands for error
	q, args, err := BuildInsert(collection, doc)
	if err != nil {
		return nil, err
	}
	return s.execWithArgs(ctx, q, args)
}

// GetRecord fetches a single record by its _id using a parameterized query.
func (s *service) GetRecord(ctx context.Context, collection, id string) (any, error) {
	// Use parameterized query to avoid injection issues
	// q stands for query
	q := fmt.Sprintf("SELECT * FROM %s WHERE _id == :id LIMIT 1", escapeIdent(collection))
	return s.execWithArgs(ctx, q, map[string]any{"id": id})
}

// GetRecords returns documents with optional LIMIT and ORDER BY.
func (s *service) GetRecords(
	// ctx stands for context
	// collection stands for Ditto collection name
	// limit stands for maximum number of records to return (0 means no limit)
	// sortBy stands for field to sort by (empty means no sorting)
	// sortOrder stands for sort direction ("ASC" or "DESC"; empty means default)
	ctx context.Context,
	collection string,
	limit int,
	sortBy, sortOrder string,
) (any, error) {
	q := BuildSelect(collection, nil, limit, sortBy, sortOrder)
	return s.execWithArgs(ctx, q, nil)
}

// UpdateRecord applies a JSON patch (field map) to a record by _id using
// parameterized SET clauses and :id.
func (s *service) UpdateRecord(
	// ctx stands for context
	// collection stands for Ditto collection name
	// id stands for record identifier (_id)
	// patch stands for map of fields to update
	ctx context.Context,
	collection, id string,
	patch map[string]any,
) (any, error) {
	q, args, err := BuildUpdate(collection, id, patch)
	if err != nil {
		return nil, err
	}
	return s.execWithArgs(ctx, q, args)
}

// DeleteRecord removes a single record by _id.
func (s *service) DeleteRecord(ctx context.Context, collection, id string) (any, error) {
    // Use parameterized query to avoid injection issues
    // q stands for query
    // Pattern A (previous): EVICT with equality operator (commented out)
    // q := fmt.Sprintf("EVICT FROM %s WHERE _id == :id", escapeIdent(collection))
    // Pattern B (current): DELETE with single equals to match curl example
    q := fmt.Sprintf("DELETE FROM %s WHERE _id = :id", escapeIdent(collection))
    return s.execWithArgs(ctx, q, map[string]any{"id": id})
}

// DeleteAllRecords removes all documents in a collection using a broad WHERE
// clause. Ditto DQL has no TRUNCATE; use DELETE with LIKE to match all ids.
func (s *service) DeleteAllRecords(ctx context.Context, collection string) (any, error) {
    if collection == "" {
        return nil, errors.New("collection required")
    }
    // Pattern A (previous): EVICT with LIKE (commented out)
    // q := fmt.Sprintf("EVICT FROM %s WHERE _id LIKE :pattern", escapeIdent(collection))
    // Pattern B (current): DELETE with LIKE
    q := fmt.Sprintf("DELETE FROM %s WHERE _id LIKE :pattern", escapeIdent(collection))
    return s.execWithArgs(ctx, q, map[string]any{"pattern": "%"})
}

// LatestRecord returns the most recent record according to the provided field
// (descending order), limited to a single result.
func (s *service) LatestRecord(ctx context.Context, collection, sortBy string) (any, error) {
	// sortBy required
	// q stands for query
	q := BuildSelect(collection, nil, 1, sortBy, "DESC")
	return s.execWithArgs(ctx, q, nil)
}

// Search builds a simple exact-match WHERE clause from the provided filters
// and applies optional LIMIT and ORDER BY.
func (s *service) Search(
	// ctx stands for context
	// collection stands for Ditto collection name
	// filters stands for map of field-value pairs for exact matches
	// limit stands for maximum number of records to return (0 means no limit)
	// sortBy stands for field to sort by (empty means no sorting)
	// sortOrder stands for sort direction ("ASC" or "DESC"; empty means default)
	ctx context.Context,
	collection string,
	filters map[string]string,
	limit int,
	sortBy, sortOrder string,
) (any, error) {
	// Build SELECT with WHERE clauses for each filter
	// q stands for query
	q := BuildSelect(collection, filters, limit, sortBy, sortOrder)
	return s.execWithArgs(ctx, q, nil)
}

// exec posts a raw DQL query without additional arguments to Ditto's
// /execute endpoint and decodes the JSON response.
func (s *service) exec(ctx context.Context, query string) (any, error) {
	// Post to /{appID}/execute
	// On non-2xx responses, return an error including an excerpt of both
	// url status code, Ditto's error response body, and the original DQL
	// payload stands for request payload
	// b stands for byte slice of JSON payload
	// req stands for HTTP request
	// resp stands for HTTP response
	url := fmt.Sprintf("%s/%s/execute", strings.TrimRight(s.BaseURL, "/"), s.AppID)
	payload := map[string]string{"query": query}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	// Set content type and execute request
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return nil, err
	}

	// Handle response
	// Close body when done
	// Check for non-2xx status codes
	// Read response body for error snippet
	// Trim and limit snippet length
	// Trim and limit query length for error message
	// Return formatted error
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		snippet := string(body)
		if len(snippet) > 256 {
			snippet = snippet[:256] + "..."
		}
		q := query
		if len(q) > 200 {
			q = q[:200] + "..."
		}
		return nil, fmt.Errorf(
			"ditto http %d: %s | query: %s",
			resp.StatusCode,
			strings.TrimSpace(snippet),
			q,
		)
	}
	var out any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// execWithArgs posts a DQL query and a query_args map to Ditto's /execute
// endpoint. On non-2xx responses, it returns an error including an excerpt
// of both Ditto's error response body and the original DQL.
func (s *service) execWithArgs(
	ctx context.Context,
	query string,
	args map[string]any,
) (any, error) {
	// Post to /{appID}/execute with query_args
	// On non-2xx responses, return an error including an excerpt of both
	// url status code, Ditto's error response body, and the original DQL
	// payload stands for request payload
	// b stands for byte slice of JSON payload
	// req stands for HTTP request
	// resp stands for HTTP response
	url := fmt.Sprintf("%s/%s/execute", strings.TrimRight(s.BaseURL, "/"), s.AppID)
	payload := map[string]any{"query": query}
	if args != nil {
		payload["query_args"] = args
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle response
	// Close body when done
	// Check for non-2xx status codes
	// Read response body for error snippet
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		snippet := string(body)
		if len(snippet) > 256 {
			snippet = snippet[:256] + "..."
		}
		q := query
		if len(q) > 200 {
			q = q[:200] + "..."
		}
		return nil, fmt.Errorf(
			"ditto http %d: %s | query: %s",
			resp.StatusCode,
			strings.TrimSpace(snippet),
			q,
		)
	}
	var out any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// Query builders ----------------------------------------------------------------

// BuildSelect constructs a DQL SELECT statement for the provided collection
// with optional exact-match filters, limit, and ordering. Identifiers are
// minimally escaped to avoid common syntax issues.
func BuildSelect(
	collection string,
	filters map[string]string,
	limit int,
	sortBy, sortOrder string,
) string {
	// collection required
	// b stands for strings.Builder to build the query
	// i stands for index for AND clauses
	var b strings.Builder
	b.WriteString("SELECT * FROM ")
	b.WriteString(escapeIdent(collection))
	if len(filters) > 0 {
		b.WriteString(" WHERE ")
		i := 0
		for k, v := range filters {
			if i > 0 {
				b.WriteString(" AND ")
			}
			b.WriteString(escapeIdent(k))
			b.WriteString(" == \"")
			b.WriteString(escapeString(v))
			b.WriteString("\"")
			i++
		}
	}
	// Optional ORDER sortBy
	// and sortOrder ("ASC" or "DESC")
	// and LIMIT limit
	if sortBy != "" {
		b.WriteString(" ORDER BY ")
		b.WriteString(escapeIdent(sortBy))
		if strings.ToUpper(sortOrder) == "DESC" {
			b.WriteString(" DESC")
		} else if strings.ToUpper(sortOrder) == "ASC" {
			b.WriteString(" ASC")
		}
	}
	if limit > 0 {
		b.WriteString(" LIMIT ")
		b.WriteString(fmt.Sprintf("%d", limit))
	}
	return b.String()
}

// BuildInsert constructs an INSERT DQL with a parameterized document (:doc).
// The returned args map contains the bound document under the "doc" key.
func BuildInsert(collection string, doc map[string]any) (string, map[string]any, error) {
	// collection required
	// doc required
	if collection == "" {
		return "", nil, errors.New("collection required")
	}
	// Use parameterized document to satisfy Ditto server requirements
	return fmt.Sprintf(
			"INSERT INTO %s DOCUMENTS (:doc)",
			escapeIdent(collection),
		), map[string]any{
			"doc": doc,
		}, nil
}

// BuildUpdate constructs an UPDATE DQL with parameterized SET clauses and
// a bound :id for the target record.
func BuildUpdate(collection, id string, patch map[string]any) (string, map[string]any, error) {
	// collection and id required
	// patch required and non-empty
	if collection == "" || id == "" {
		return "", nil, errors.New("collection and id required")
	}
	// Convert patch map into SET syntax using JSON marshaling for values
	if len(patch) == 0 {
		return "", nil, errors.New("patch is empty")
	}
	// parts collects SET clauses
	var parts []string
	args := map[string]any{"id": id}
	for k, v := range patch {
		pname := fmt.Sprintf("p_%s", k)
		parts = append(parts, fmt.Sprintf("%s = :%s", escapeIdent(k), pname))
		args[pname] = v
	}
	set := strings.Join(parts, ", ")
	return fmt.Sprintf("UPDATE %s SET %s WHERE _id == :id", escapeIdent(collection), set), args, nil
}

// escapeIdent performs minimal identifier sanitization suitable for DQL.
// It removes backticks and replaces spaces with underscores.
func escapeIdent(s string) string {
	// collection and field names should be simple identifiers
	// Very basic identifier safety: replace backticks and spaces
	s = strings.ReplaceAll(s, "`", "")
	s = strings.ReplaceAll(s, " ", "_")
	return s
}

// escapeString escapes double quotes in a string literal.
func escapeString(s string) string {
	// Escape double quotes
	return strings.ReplaceAll(s, "\"", "\\\"")
}

// Docker integration -----------------------------------------------------------

// DockerRunner abstracts container lifecycle operations so the service can run
// with either plain Docker or Docker Compose backends.
type DockerRunner interface {
	// EnsureImageLoaded checks for the specified Docker image locally and
	// loads it from a tarball if it is missing. If tarPath is empty, it
	// assumes the image is available or will be pulled by other means.
	EnsureImageLoaded(ctx context.Context, imageName, tarPath string) error
	ContainerStatus(ctx context.Context, name string) (string, error)
	RunContainer(ctx context.Context, opts DockerOptions) error
	StartContainer(ctx context.Context, name string) error
	StopContainer(ctx context.Context, name string) error
}

// DockerOptions collects parameters for starting a Ditto Edge container.
type DockerOptions struct {
	// Required settings
	ContainerName string
	ImageName     string
	ImageTarPath  string
	ConfigPath    string
	DataPath      string
	// Optional docker compose settings
	ComposeFile    string // path to docker-compose.yml; empty means default discovery
	ComposeService string // service name; defaults to "ditto-edge-server" if empty
}

// dockerRunnerDefault implements DockerRunner via plain Docker CLI commands.
type dockerRunnerDefault struct{}

// NewDockerRunnerDefault returns a DockerRunner that manages containers using
// `docker` commands (no Compose integration).
func NewDockerRunnerDefault() DockerRunner { return &dockerRunnerDefault{} }

// EnsureImageLoaded checks for an image locally and loads it from a tarball
// if it is missing. When tarPath is empty, it assumes the image is available
// or will be pulled by other means.
func (d *dockerRunnerDefault) EnsureImageLoaded(
	ctx context.Context,
	imageName, tarPath string,
) error {
	// Check if image exists
	if err := runCmd(ctx, "docker", "image", "inspect", imageName); err == nil {
		return nil
	}
	// Load from tar
	if err := runCmd(ctx, "docker", "load", "-i", tarPath); err != nil {
		return fmt.Errorf("docker load: %w", err)
	}
	return nil
}

// ContainerStatus returns a coarse status for the container: running, exited,
// not-found, or a raw status string from `docker ps`.
func (d *dockerRunnerDefault) ContainerStatus(ctx context.Context, name string) (string, error) {
	// Use docker ps to check status by container name
	// Possible results:
	// not-found (no such container)
	// exited (created but stopped)
	// running (up)

	// running, exited, or not-found
	cmd := exec.CommandContext(
		ctx,
		"bash",
		"-lc",
		fmt.Sprintf("docker ps -a --filter name=^/%s$ --format '{{.Status}}'", name),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker ps: %w", err)
	}
	s := strings.ToLower(strings.TrimSpace(string(out)))
	if s == "" {
		return "not-found", nil
	}
	if strings.HasPrefix(s, "up ") {
		return "running", nil
	}
	if strings.HasPrefix(s, "exited ") {
		return "exited", nil
	}
	return s, nil
}

// RunContainer starts a new Ditto Edge container using `docker run` wiring the
// config and data mounts and exposing the HTTP API port.
func (d *dockerRunnerDefault) RunContainer(ctx context.Context, opts DockerOptions) error {
	// Run new container with config and data mounts
	// Expose port 8090 on localhost only
	// Ditto Edge server command: run -c /config.yaml
	// args stands for docker run arguments
	// fmt stands for format
	// If any required options are missing, return an error
	args := []string{
		"run", "-d", "--name", opts.ContainerName,
		"-p", "127.0.0.1:8090:8090",
		"-v", fmt.Sprintf("%s:/config.yaml", opts.ConfigPath),
		"-v", fmt.Sprintf("%s:/data", opts.DataPath),
		opts.ImageName, "run", "-c", "/config.yaml",
	}
	if err := runCmd(ctx, "docker", args...); err != nil {
		return fmt.Errorf("docker run: %w", err)
	}
	return nil
}

// StartContainer starts a previously created container.
func (d *dockerRunnerDefault) StartContainer(ctx context.Context, name string) error {
	return runCmd(ctx, "docker", "start", name)
}

// StopContainer stops a running container.
func (d *dockerRunnerDefault) StopContainer(ctx context.Context, name string) error {
	return runCmd(ctx, "docker", "stop", name)
}

// runCmd executes a CLI command and returns a formatted error including
// stdout/stderr when the command fails.
func runCmd(ctx context.Context, name string, args ...string) error {
	// Execute command and capture combined output
	// On error, return formatted error with command, args, error, and output
	// cmd stands for exec.CommandContext
	// out stands for command output
	// err stands for error
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %v: %s", name, strings.Join(args, " "), err, string(out))
	}
	return nil
}

// docker compose-based runner --------------------------------------------------

// composeRunnerDefault implements DockerRunner using Docker Compose commands.
type composeRunnerDefault struct{}

// NewComposeRunnerDefault returns a DockerRunner backed by `docker compose`.
func NewComposeRunnerDefault() DockerRunner { return &composeRunnerDefault{} }

// EnsureImageLoaded mirrors the behavior of dockerRunnerDefault for parity.
func (d *composeRunnerDefault) EnsureImageLoaded(
	ctx context.Context,
	imageName, tarPath string,
) error {
	// Same behavior: inspect first; if not present, try to load from tar
	if err := runCmd(ctx, "docker", "image", "inspect", imageName); err == nil {
		return nil
	}
	if tarPath != "" {
		if err := runCmd(ctx, "docker", "load", "-i", tarPath); err != nil {
			return fmt.Errorf("docker load: %w", err)
		}
		return nil
	}
	// If no tar provided, let compose pull/build (no-op here)
	return nil
}

// ContainerStatus reports the status using `docker ps` for the given container
// name, which should match the `container_name` in docker-compose.yml.
func (d *composeRunnerDefault) ContainerStatus(ctx context.Context, name string) (string, error) {
	// Possible results:
	// not-found (no such container)
	// exited (created but stopped)
	// running (up)
	// running, exited, or not-found

	// Use docker ps on container_name because compose service maps to container_name
	cmd := exec.CommandContext(
		ctx,
		"bash",
		"-lc",
		fmt.Sprintf("docker ps -a --filter name=^/%s$ --format '{{.Status}}'", name),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker ps: %w", err)
	}
	s := strings.ToLower(strings.TrimSpace(string(out)))
	if s == "" {
		return "not-found", nil
	}
	if strings.HasPrefix(s, "up ") {
		return "running", nil
	}
	if strings.HasPrefix(s, "exited ") {
		return "exited", nil
	}
	return s, nil
}

// RunContainer brings the compose service up with `docker compose up -d`.
func (d *composeRunnerDefault) RunContainer(ctx context.Context, opts DockerOptions) error {
	// Use docker compose up -d [service]
	// If ComposeFile is provided, use -f to specify it
	// If ComposeService is empty, default to "ditto-edge-server"
	svc := opts.ComposeService
	if svc == "" {
		svc = "ditto-edge-server"
	}
	args := []string{"compose"}
	if opts.ComposeFile != "" {
		args = append(args, "-f", opts.ComposeFile)
	}
	args = append(args, "up", "-d", svc)
	if err := runCmd(ctx, "docker", args...); err != nil {
		return fmt.Errorf("docker compose up: %w", err)
	}
	return nil
}

// StartContainer starts a stopped compose service.
func (d *composeRunnerDefault) StartContainer(ctx context.Context, name string) error {
	// Use docker compose start [service]
	// If ComposeFile is provided, use -f to specify it
	// If ComposeService is empty, default to "ditto-edge-server"
	args := []string{"compose", "start", name}
	return runCmd(ctx, "docker", args...)
}

// StopContainer stops the compose service and then best-effort stops/removes
// any lingering container by name.
func (d *composeRunnerDefault) StopContainer(ctx context.Context, name string) error {
	// Use docker compose stop [service]
	// If ComposeFile is provided, use -f to specify it
	// If ComposeService is empty, default to "ditto-edge-server"
	// Then best-effort stop/remove any lingering container by name
	// Compose stop may leave the container running, so ensure it's stopped
	// and removed
	// Ignore errors during cleanup
	// args stands for docker compose arguments

	// Best effort: stop via compose, then ensure container is removed
	_ = runCmd(ctx, "docker", "compose", "stop", name)
	_ = runCmd(ctx, "docker", "stop", name)
	_ = runCmd(ctx, "docker", "rm", "-f", name)
	return nil
}
