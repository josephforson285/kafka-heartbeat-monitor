# A thin wrapper over the commands that are long enough to get retyped wrong.
# Anything already served well by `heartbeat --help` is deliberately absent: there
# should not be two ways to start a producer.

SHELL   := bash
# the repo venv when there is one, otherwise whatever python is on the path — CI
# installs into the runner's interpreter and has no .venv
PY      := $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
COMPOSE := docker compose

# Some shells export a PYTHONPATH that leaks unrelated site-packages into the venv.
# Clearing it here makes a recipe behave the same whoever runs it.
export PYTHONPATH :=

-include .env
POSTGRES_USER     ?= heartbeat
POSTGRES_PASSWORD ?= heartbeat
GROUP             ?= heartbeat-writer

.DEFAULT_GOAL := help
.PHONY: help up down reset test test-all proofs lag psql

help:  ## list the targets
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sort | awk -F':.*##' '{printf "  %-10s %s\n", $$1, $$2}'

up:  ## start the stack and wait until every container is healthy
	@$(COMPOSE) up -d
	@printf 'waiting for the stack'
	@for _ in $$(seq 90); do \
	  [ "$$($(COMPOSE) ps --format '{{.Health}}' | grep -c healthy)" -eq 5 ] && break; \
	  printf '.'; sleep 2; \
	done; printf '\n'
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}'

down:  ## stop the stack, keep the data
	$(COMPOSE) down

reset:  ## stop and delete everything, orphans from the single-broker layout included
	$(COMPOSE) down -v --remove-orphans

test:  ## run the test suite (integration tests skip unless HEARTBEAT_TEST_DSN is set)
	$(PY) -m pytest -q

test-all:  ## run every test, creating a disposable database for the integration ones
	@$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d postgres \
	  -c "CREATE DATABASE heartbeat_test;" >/dev/null 2>&1 || true
	@HEARTBEAT_TEST_DSN="host=localhost port=5434 dbname=heartbeat_test user=$(POSTGRES_USER) password=$(POSTGRES_PASSWORD)" \
	 HEARTBEAT_ALLOW_DESTRUCTIVE_TESTS=1 $(PY) -m pytest -q

proofs:  ## run the failure-mode proofs — destructive, resets the stack
	./scripts/demo_failure_modes.sh

lag:  ## consumer lag per partition (override with GROUP=name)
	@$(COMPOSE) exec -T kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka1:19092 --describe --group $(GROUP)

psql:  ## interactive shell on the database
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d heartbeat
