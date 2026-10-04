.PHONY: up down logs demo crash-demo graceful-shutdown-test semantics-test observability-test index-review clean-room-test load benchmark test test-race test-store-integration test-integration fmt-check vet verify clean

GO_IMAGE := golang:1.23-alpine
RACE_GO_IMAGE := golang:1.23

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f api worker-a worker-b

demo:
	./scripts/demo.sh

crash-demo:
	./scripts/crash-recovery-demo.sh

graceful-shutdown-test:
	./scripts/graceful-shutdown-test.sh

semantics-test:
	./scripts/workflow-semantics-test.sh

observability-test:
	./scripts/observability-test.sh

index-review:
	./scripts/index-review.sh

clean-room-test:
	./scripts/clean-room-test.sh

load:
	docker compose --profile tools run --rm loadgen -workflows 200 -concurrency 32

benchmark:
	./scripts/benchmark.sh

test:
	docker run --rm -v "$(CURDIR):/src" -w /src $(GO_IMAGE) go test ./...

test-race:
	docker run --rm -v "$(CURDIR):/src" -w /src $(RACE_GO_IMAGE) go test -race ./...

fmt-check:
	docker run --rm -v "$(CURDIR):/src" -w /src $(GO_IMAGE) sh -c 'test -z "$$(gofmt -l .)"'

vet:
	docker run --rm -v "$(CURDIR):/src" -w /src $(GO_IMAGE) go vet ./...

test-integration:
	docker compose up --build -d db api worker-a worker-b
	./scripts/integration-test.sh

test-store-integration:
	docker compose up --build -d db api
	docker compose stop worker-a worker-b
	docker compose --profile tools run --rm test go test -tags=integration ./internal/store
	docker compose up -d worker-a worker-b

verify: fmt-check vet test test-race test-store-integration test-integration crash-demo graceful-shutdown-test semantics-test observability-test index-review

clean:
	docker compose down -v --remove-orphans
