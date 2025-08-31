# Ditto SDK for Go

A thin client for interacting with a Ditto Edge HTTP API, with optional Docker/Compose helpers to manage the Ditto Edge container lifecycle.

- Module: `github.com/Hammerstone-AU/ditto-go-sdk`
- Minimum Go: 1.22
- Status: v0.1.0 (initial drop)

## Install

After you push to GitHub under `Hammerstone-AU/ditto-go-sdk`:

```bash
go get github.com/Hammerstone-AU/ditto-go-sdk@latest
```

If you're integrating **before** pushing to GitHub, use a `replace` in your project's `go.mod`:

```go
replace github.com/Hammerstone-AU/ditto-go-sdk => ../path/to/ditto-go-sdk
```

## Quickstart

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/Hammerstone-AU/ditto-go-sdk/ditto"
)

func main() {
    svc := ditto.NewService("http://localhost:8090", "myapp")
    // Optional: manage Ditto container using Docker
    docker := ditto.NewDockerRunnerDefault() // or ditto.NewComposeRunnerDefault()
    svc.WithDocker(docker, ditto.DockerOptions{
        ContainerName: "ditto-edge",
        ImageName:     "dittoedge/server:latest",
        ImageTarPath:  "",                  // optional tarball path
        ConfigPath:    "/path/to/config.yaml",
        DataPath:      "/path/to/data",
        // ComposeFile:    "/path/to/docker-compose.yml",
        // ComposeService: "ditto-edge-server",
    })

    if err := svc.InitDB(context.Background()); err != nil {
        log.Fatal(err)
    }
    defer svc.Close(context.Background())

    // Insert a doc
    res, err := svc.CreateDocument(context.Background(), "users", map[string]any{
        "name": "Alice",
        "age":  30,
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Insert: %#v\n", res)

    // Fetch latest
    latest, err := svc.LatestRecord(context.Background(), "users", "_id")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Latest: %#v\n", latest)
}
```

## Features

- Safe, parameterised DQL (`INSERT`, `SELECT`, `UPDATE`, `DELETE`)
- Simple search, pagination via `LIMIT`, and ordering
- Docker runner (`docker` CLI) and Compose runner (`docker compose`) helpers
- Minimal dependencies (std lib only)

## API surface

```text
type Service interface {
    InitDB(ctx context.Context) error
    Close(ctx context.Context) error
    Status(ctx context.Context) (map[string]any, error)
    CreateDocument(ctx context.Context, collection string, doc map[string]any) (any, error)
    GetRecord(ctx context.Context, collection, id string) (any, error)
    GetRecords(ctx context.Context, collection string, limit int, sortBy, sortOrder string) (any, error)
    UpdateRecord(ctx context.Context, collection, id string, patch map[string]any) (any, error)
    DeleteRecord(ctx context.Context, collection, id string) (any, error)
    DeleteAllRecords(ctx context.Context, collection string) (any, error)
    LatestRecord(ctx context.Context, collection, sortBy string) (any, error)
    Search(ctx context.Context, collection string, filters map[string]string, limit int, sortBy, sortOrder string) (any, error)
}
```

## Pushing to GitHub

```bash
cd ditto-go-sdk
git init
git add .
git commit -m "chore: initial Ditto Go SDK"
git branch -M main
git remote add origin git@github.com:Hammerstone-AU/ditto-go-sdk.git
git push -u origin main
# Optionally add a version tag
git tag v0.1.0
git push origin v0.1.0
```

## Notes

- Docker is optional; if you already run Ditto elsewhere, skip `WithDocker` and `InitDB` will be a no-op.
- Ensure `docker` / `docker compose` CLIs are available if you enable container management.
