.PHONY: all test test-v clean run

DEMO_PATH := examples/demo

all: test

test:
	go test ./...

run:
	@echo "Running demo example..."
	go run $(DEMO_PATH)/main.go

test-v:
	go test -v ./...

race:
	go test -race ./...

race-v:
	go test -v -race ./...

clean:
	go clean -testcache
	rm -rf demodb
