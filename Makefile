BINARY := dd
BIN_DIR := bin

.PHONY: build run test fmt vet tidy clean

build:
	go build -o $(BIN_DIR)/$(BINARY) .

run:
	go run . $(ARGS)

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -rf $(BIN_DIR)
