# dlock

`dlock` is a library which provides safe exclusive locking in distributed systems, based on well synchronized clocks and
AWS DynamoDB - making it the perfect choice for your AWS Lambda functions.


## Features

### Safety with fencing tokens

Locks returned by `dlock` ensure that no other parallel process can acquire the same lock. Since it is built for
distributed systems, there are some situations where `dlock` cannot be sure if it still has the lock (e.g. network
issues to DynamoDB) - in these cases `dlock` always errs on the side of caution, rather marking a lock as expired than
proceeding if unsure. Note that this guarantee only holds as long as either fencing tokens are used or the process
holding the lock does not get paused for a time greater than the lease time. See 
[Martin Kleppmanns post](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html#protecting-a-resource-with-a-lock) 
for more details on distributed locking and fencing tokens. `dlock` supports generating fencing tokens.

The safety guarantees are ultimately based on the guarantees that DynamoDB gives for conditional writes, see 
[AWS docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate).
`dlock` only executes conditional writes.

### Absolute timestamps

`dlock` stores absolute timestamps in DynamoDB for each active lock ("lease until"), in contrast to other libraries
which store the duration during which the lock is active. The participating clients in these other libraries need to 
(actively) wait the lease duration of the lock, until they are safe to steal the lock - imagine a process that crashed 
before it was able to release the lock. Choosing to store absolute timestamps not only improves
throughput, but also allows to use `dlock` in environments where runtimes of programs can be very short-lived, e.g. 
**AWS Lambda**. Having Lambdas run for some time just to wait for a duration to pass in order to proceed on a stale lock
incurs costs that can easily be prevented.

This requires that the clocks of the systems that use `dlock` have to be synchronized well and must not "jump". Use NTP
or some other mechanism to keep the clocks in sync and use `WithMaxClockSkew` to define the maximum amount of time that
clocks might differ from each other.

Since AWS Lambda is a managed service that 
[synchronizes the system clocks](https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime),
you do not need to care about this when using `dlock`. 


### Automatic heartbeats

A lock once acquired by `dlock` stays valid until it is unlocked. In order to refresh the absolute "lease until"
timestamp in DynamoDB, automatic heartbeats are in place. If these keep failing, e.g. due to unavailability of DynamoDB,
the locks will expire.

### Warning before locks are about to expire

Each lock has a "WarnChan" which posts a message at a configurable time before the lock expires. This gives the process
time to gracefully shut down all operations it might be doing where it relies on having the exclusive lock.

## Usage

1. Get it ```go get -u https://github.com/scailio-oss/dlock```
2. Create DynamoDB table with a partition key of type string and name `key`. Default name of the table is `dlock`.
3. Use:

```go
package main

import (
	// snip

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/scailio-oss/dlock"
)

func main() {
	awsConfig := aws.Config{} // Whatever you need to create the config
	dynamoDbClient := dynamodb.NewFromConfig(awsConfig)

	// Ensure this is unique for this program instance. E.g. use AWS RequestId in Lambda.
	ownerName := strconv.FormatUint(rand.Uint64(), 16)

	locker := dlock.NewLocker(dynamoDbClient, ownerName,
		// This locker locks objects of type 'streets in NYC'
		dlock.WithLockIdPrefix("nyc-street-"),
		dlock.WithLease(10*time.Second),
		dlock.WithHeartbeat(2*time.Second),
		dlock.WithMaxClockSkew(10*time.Second),
		dlock.WithWarnAfter(9*time.Second),
		dlock.WithDynamoDbTimeout(1*time.Second),
	)
	defer locker.Close()

	// Try to acquire a lock on 'wallstreet'
	lock, err := locker.TryLock(context.Background(), "wallstreet")
	if err != nil {
		fmt.Printf("Could not lock: %v\n", err)
		return
	}

	// TODO do things exclusively on object 'wallstreet'

	select {
	case <-lock.WarnChan():
		// Will fire after 9 seconds after the last successful heartbeat
		fmt.Printf("WARNING, the lock is about to expire\n")
	default:
	}

	lock.Unlock(context.Background())
}
```

## Integration tests

`dlock` contains integration tests, which locally start a DynamoDB in a Docker container. See 
[internal/itest/](internal/itest/) for details.

## Comparison to other libraries

* https://github.com/cirello-io/dynamolock v2
  * Instead of an absolute "lease until" timestamp, `dynamolock` stores a "duration" in the database with the downsides
    for short-lived processes as noted above
  * Does not generate fencing tokens
* https://github.com/Clever/dynamodb-lock-go 
  * `dynamo-lock-go` uses AWS SDK v1 instead of v2
  * Does not generate fencing tokens

## License

Apache 2.0