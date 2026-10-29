SHELL := /bin/sh

TOPIC ?= dhan.market.raw
BROKER ?= localhost:19092

.PHONY: up down topic mock inspect test vet fmt

up:
	docker compose up -d

down:
	docker compose down

topic:
	docker compose exec -T redpanda rpk topic describe $(TOPIC) --brokers redpanda:9092 >/dev/null 2>&1 || docker compose exec -T redpanda rpk topic create $(TOPIC) --brokers redpanda:9092

mock:
	go run ./dhan-redpanda-starter/cmd/mock --broker $(BROKER) --topic $(TOPIC) --count 100

inspect:
	docker compose exec -T redpanda rpk topic describe $(TOPIC) --brokers redpanda:9092
	docker compose exec -T redpanda rpk topic consume $(TOPIC) --offset start --num 100 --brokers redpanda:9092 --format '%v\n'

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w $$(rg --files -g '*.go')
