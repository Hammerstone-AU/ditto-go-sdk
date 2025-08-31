package main

import (
    "context"
    "fmt"
    "log"

    "github.com/Hammerstone-AU/ditto-go-sdk/ditto"
)

func main() {
    service := ditto.NewService("http://localhost:8090", "exampledb")

    // Optional: enable Docker management (uncomment and set paths)
    // docker := ditto.NewDockerRunnerDefault()
    // service.WithDocker(docker, ditto.DockerOptions{
    //     ContainerName: "ditto-edge",
    //     ImageName:     "dittoedge/server:latest",
    //     ImageTarPath:  "",
    //     ConfigPath:    "/absolute/path/to/config.yaml",
    //     DataPath:      "/absolute/path/to/data",
    // })

    // Basic insert/select
    doc := map[string]any{"hello": "world"}
    if _, err := service.CreateDocument(context.Background(), "greetings", doc); err != nil {
        log.Fatal(err)
    }
    out, err := service.GetRecords(context.Background(), "greetings", 10, "_id", "DESC")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Query result: %#v\n", out)
}
