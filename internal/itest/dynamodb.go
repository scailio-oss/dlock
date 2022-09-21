/*
 *    Copyright 2022 scailio GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package itest

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

// Starts a local DynamoDB in a docker container, creates a dynamoDb client for it and creates the table that Locker needs.
// Returns the dynamoDB client and a shutdown function which must be called to shut down and remove the docker container
// as soon as it is not needed anymore.
func startDynamoDb() (*dynamodb.Client, func()) {
	// ===== STEP 1: Download and start DynamoDB container
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	fmt.Printf("Pulling dynamodb-local docker image\n")

	reader, err := cli.ImagePull(ctx, "amazon/dynamodb-local:latest", dockertypes.ImagePullOptions{})
	if err != nil {
		panic(err)
	}

	//goland:noinspection GoUnhandledErrorResult
	defer reader.Close()
	//goland:noinspection GoUnhandledErrorResult
	io.Copy(os.Stdout, reader)

	createResponse, err := cli.ContainerCreate(ctx, &container.Config{
		Image: "amazon/dynamodb-local:latest",
		ExposedPorts: nat.PortSet{
			"8000/tcp": struct{}{},
		},
		Tty: false,
	}, &container.HostConfig{
		PortBindings: nat.PortMap{
			"8000/tcp": []nat.PortBinding{
				{
					HostIP: "localhost",
				},
			},
		},
	}, nil, nil, "")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Created docker container: %v\n", createResponse.ID)

	if err := cli.ContainerStart(ctx, createResponse.ID, dockertypes.ContainerStartOptions{}); err != nil {
		panic(err)
	}

	var hostPort uint16
	if containerList, err := cli.ContainerList(context.Background(), dockertypes.ContainerListOptions{
		Filters: filters.NewArgs(
			filters.Arg("id", createResponse.ID),
		),
	}); err != nil || len(containerList) != 1 {
		fmt.Printf("Could not list container in order to find port. Result: %v\n", containerList)
		panic(err)
	} else {
		hostPort = containerList[0].Ports[0].PublicPort
		fmt.Printf("Found host port: %v\n", hostPort)
	}

	shutdown := func() {
		fmt.Printf("************** Output of DynamoDB container **************\n")
		if out, err := cli.ContainerLogs(ctx, createResponse.ID, dockertypes.ContainerLogsOptions{ShowStdout: true}); err != nil {
			fmt.Printf("ERROR getting container logs: %v\n", err)
		} else {
			//goland:noinspection GoUnhandledErrorResult
			stdcopy.StdCopy(os.Stdout, os.Stderr, out)
		}
		fmt.Printf("************** Output of DynamoDB container end **************\n")

		fmt.Printf("Removing container\n")
		err = cli.ContainerRemove(context.Background(), createResponse.ID, dockertypes.ContainerRemoveOptions{
			RemoveVolumes: true,
			Force:         true,
		})
		if err != nil {
			fmt.Printf("Error removing container %v: %v\n", createResponse.ID, err)
		}
	}

	// ===== STEP 2: Initialize a DynamoDB client, create the table and block until it is created
	config := aws.NewConfig()
	config.Region = "eu-west-1"
	config.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: fmt.Sprintf("http://localhost:%v/", hostPort)}, nil
		})
	config.Credentials = credentials.StaticCredentialsProvider{Value: aws.Credentials{
		AccessKeyID:     "dummy",
		SecretAccessKey: "dummy",
		SessionToken:    "dummy",
		Source:          "not needed for local dynamodb",
	}}

	dynamoDbClient := dynamodb.NewFromConfig(*config)

	// create the default table "dlock" with PK "key" of type string.
	createTableOutput, err := dynamoDbClient.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{{
			AttributeName: aws.String("key"),
			AttributeType: dynamodbtypes.ScalarAttributeTypeS,
		}},
		KeySchema: []dynamodbtypes.KeySchemaElement{{
			AttributeName: aws.String("key"),
			KeyType:       dynamodbtypes.KeyTypeHash,
		}},
		TableName:   aws.String("dlock"),
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})

	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		shutdown()
		panic(err)
	}

	tableStatus := createTableOutput.TableDescription.TableStatus
	for tableStatus != dynamodbtypes.TableStatusActive {
		dto, err := dynamoDbClient.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
			TableName: aws.String("dlock"),
		})
		if err != nil {
			fmt.Printf("ERROR describing dynamoDB table. Will retry. %v\n", err)
			continue
		}
		tableStatus = dto.Table.TableStatus
	}

	fmt.Printf("DynamoDB started and table created successfully.\n")

	return dynamoDbClient, shutdown
}
