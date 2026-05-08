.PHONY: all test test-v clean

all: test

test:
	go test ./...

test-v:
	go test -v ./...

race:
	go test -race ./...

race-v:
	go test -v -race ./...

clean:
	go clean -testcache
