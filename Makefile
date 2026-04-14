# =============================================================================
# Flink Clickstream Event Reordering – Local Dev Makefile
# =============================================================================
#
# Quick start:
#   make build          # compile the fat JAR
#   make setup          # start Kafka + Flink, create topics
#   make submit-job     # upload JAR and run the Flink job
#   make produce N=200  # send 200 fake clickstream events
#   make consume        # tail the checkout-session output topic (Ctrl-C to exit)
#   make teardown       # stop everything and wipe volumes
#
# Override any variable on the command line:
#   make produce N=50 CHECKOUT_RATIO=0.8

# ---------------------------------------------------------------------------
# Variables (override on the command line as needed)
# ---------------------------------------------------------------------------

# podman-compose binary – switch to "podman compose" (no hyphen) if your
# installation requires it.
COMPOSE         ?= podman-compose

# Auto-detect the container platform based on the host CPU architecture.
# Override on the command line if needed: make setup PLATFORM=linux/arm64
_ARCH           := $(shell uname -m)
ifeq ($(_ARCH),arm64)
  PLATFORM      ?= linux/arm64
else ifeq ($(_ARCH),aarch64)
  PLATFORM      ?= linux/arm64
else
  PLATFORM      ?= linux/amd64
endif
export PLATFORM

# Python interpreter used to create the virtualenv
PYTHON          ?= python3

# Number of fake events to produce  (make produce N=500)
N               ?= 100

# Producer tuning knobs
CHECKOUT_RATIO  ?= 0.7   # fraction of sessions that include a Checkout
LATE_RATIO      ?= 0.3   # fraction of events sent out-of-order
MAX_LATE_SECS   ?= 300   # max arrival delay in seconds

# Kafka addresses
#   HOST_KAFKA    – reachable from the host (external listener)
#   INTERNAL_KAFKA – reachable from inside containers
HOST_KAFKA      ?= localhost:9094
INTERNAL_KAFKA  ?= kafka:9092

# Topic names
INPUT_TOPIC       ?= clickstream
OUTPUT_TOPIC      ?= checkout-session
PRICE_STATS_TOPIC ?= price-stats

# Flink REST endpoint
FLINK_REST      ?= http://localhost:8081

# Directory (inside the Flink containers) where savepoints are written.
# Both JobManager and TaskManager must be able to write here.
SAVEPOINT_DIR   ?= /tmp/flink-savepoints

# Host-side file that stores the last savepoint path (created by savepoint-stop,
# consumed by restart-from-savepoint).
SAVEPOINT_PATH_FILE := .savepoint-path

# Fat JAR produced by Maven
JAR             := target/clickstream-event-reordering-1.0-SNAPSHOT.jar

# Python virtualenv paths
VENV            := scripts/.venv
VENV_BIN        := $(VENV)/bin
VENV_MARKER     := $(VENV)/.installed   # sentinel file; rebuilt only when requirements change

# Kafka CLI inside the Bitnami container
KAFKA_BIN       := /opt/kafka/bin
KAFKA_EXEC      := podman exec clickstream-kafka $(KAFKA_BIN)

# ---------------------------------------------------------------------------
# Phony targets
# ---------------------------------------------------------------------------
.PHONY: help build up down setup teardown \
        wait-for-kafka wait-for-flink create-topics list-topics \
        venv produce consume consume-price-stats \
        submit-job list-jobs cancel-jobs savepoint-stop restart-from-savepoint \
        logs-jm logs-tm status clean \
        shell-kafka shell-kafka-ui shell-jm shell-tm \
        stop-flink

# ---------------------------------------------------------------------------
# Default target – print usage
# ---------------------------------------------------------------------------
help:
	@printf '\n\033[1mFlink Clickstream Event Reordering — Local Dev\033[0m\n\n'
	@printf '\033[4mBuild\033[0m\n'
	@printf '  make build                  Compile the Maven fat JAR\n\n'
	@printf '\033[4mInfrastructure\033[0m\n'
	@printf '  make setup                  Start containers + create Kafka topics\n'
	@printf '  make up                     Start all containers (podman-compose up)\n'
	@printf '  make down                   Stop containers (keep volumes)\n'
	@printf '  make teardown               Stop containers + wipe all volumes\n'
	@printf '  make stop-flink             Stop and prune Flink jobmanager + taskmanager\n'
	@printf '  make status                 Show running container status\n\n'
	@printf '\033[4mKafka\033[0m\n'
	@printf '  make create-topics          Create input/output Kafka topics\n'
	@printf '  make list-topics            List all topics on the broker\n'
	@printf '  make produce [N=100]        Produce N fake clickstream events\n'
	@printf '  make consume                Tail the checkout-session output topic\n'
	@printf '  make consume-price-stats    Tail the price-stats topic (5-min window aggregates)\n\n'
	@printf '\033[4mFlink\033[0m\n'
	@printf '  make submit-job             Upload the fat JAR and run the job\n'
	@printf '  make list-jobs              Show running Flink jobs\n'
	@printf '  make cancel-jobs            Cancel all running Flink jobs\n'
	@printf '  make savepoint-stop         Create a savepoint and stop the running job\n'
	@printf '  make restart-from-savepoint Restart the job from the last savepoint\n'
	@printf '  make logs-jm                Tail JobManager logs\n'
	@printf '  make logs-tm                Tail TaskManager logs\n\n'
	@printf '\033[4mShells\033[0m\n'
	@printf '  make shell-kafka            bash inside the Kafka broker container\n'
	@printf '  make shell-kafka-ui         sh inside the Kafka UI container\n'
	@printf '  make shell-jm               bash inside the Flink JobManager container\n'
	@printf '  make shell-tm               bash inside the Flink TaskManager container\n\n'
	@printf '\033[4mCleanup\033[0m\n'
	@printf '  make clean                  Remove Maven build artifacts\n\n'
	@printf '\033[4mVariables\033[0m (override on the command line)\n'
	@printf '  N=100                       Number of events to produce\n'
	@printf '  CHECKOUT_RATIO=0.7          Fraction of sessions with Checkout\n'
	@printf '  LATE_RATIO=0.3              Fraction of events sent out-of-order\n'
	@printf '  HOST_KAFKA=localhost:9094   Kafka address from the host machine\n'
	@printf '  FLINK_REST=http://localhost:8081\n\n'

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

## Compile and package the fat JAR (skip tests for speed).
build:
	@printf '\n==> Building fat JAR …\n'
	mvn package -DskipTests
	@printf '\n==> Built: $(JAR)\n'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

## Start all containers in the background.
up:
	@printf '\n==> Starting containers …\n'
	$(COMPOSE) up -d
	@printf '\n==> Containers started. Kafka UI: http://localhost:8080  Flink UI: $(FLINK_REST)\n'

## Stop containers but keep the named volumes (data survives).
down:
	@printf '\n==> Stopping containers …\n'
	$(COMPOSE) down
	-podman container stop $$(podman ps -q)
	podman container prune -f
	@printf 'Done.\n'

## Start containers, wait for readiness, create topics.
setup: up wait-for-kafka create-topics wait-for-flink
	@printf '\n\033[1m==> Setup complete!\033[0m\n'
	@printf '    Kafka UI  : http://localhost:8080\n'
	@printf '    Flink UI  : $(FLINK_REST)\n'
	@printf '    Next step : make build && make submit-job && make produce\n\n'

## Stop containers and delete all named volumes (full reset).
teardown: down
	@printf '\n==> Tearing down stack and removing volumes …\n'
	$(COMPOSE) down -v
	@printf 'Done.\n'

## Show container status.
status:
	$(COMPOSE) ps

# ---------------------------------------------------------------------------
# Wait helpers (used by setup)
# ---------------------------------------------------------------------------

## Block until the Kafka broker is accepting connections.
wait-for-kafka:
	@printf '\n==> Waiting for Kafka to be ready '
	@for i in $$(seq 1 40); do \
		if $(KAFKA_EXEC)/kafka-topics.sh --list \
			--bootstrap-server localhost:9092 >/dev/null 2>&1; then \
			printf ' ✓\n'; exit 0; \
		fi; \
		printf '.'; sleep 3; \
	done; \
	printf '\nERROR: Kafka did not become ready in time.\n'; exit 1

## Block until the Flink REST API is reachable.
wait-for-flink:
	@printf '\n==> Waiting for Flink REST API '
	@for i in $$(seq 1 40); do \
		if curl -sf $(FLINK_REST)/overview >/dev/null 2>&1; then \
			printf ' ✓\n'; exit 0; \
		fi; \
		printf '.'; sleep 3; \
	done; \
	printf '\nERROR: Flink REST API did not become ready in time.\n'; exit 1

# ---------------------------------------------------------------------------
# Kafka topic management
# ---------------------------------------------------------------------------

## Create the input (clickstream), output (checkout-session), and price-stats topics.
create-topics:
	@printf '\n==> Creating Kafka topics …\n'
	$(KAFKA_EXEC)/kafka-topics.sh \
		--create --if-not-exists \
		--bootstrap-server localhost:9092 \
		--topic $(INPUT_TOPIC) \
		--partitions 4 \
		--replication-factor 1
	$(KAFKA_EXEC)/kafka-topics.sh \
		--create --if-not-exists \
		--bootstrap-server localhost:9092 \
		--topic $(OUTPUT_TOPIC) \
		--partitions 4 \
		--replication-factor 1
	$(KAFKA_EXEC)/kafka-topics.sh \
		--create --if-not-exists \
		--bootstrap-server localhost:9092 \
		--topic $(PRICE_STATS_TOPIC) \
		--partitions 4 \
		--replication-factor 1
	@printf '==> Topics ready: [$(INPUT_TOPIC)] [$(OUTPUT_TOPIC)] [$(PRICE_STATS_TOPIC)]\n'

## List all topics on the broker.
list-topics:
	$(KAFKA_EXEC)/kafka-topics.sh --list --bootstrap-server localhost:9092

# ---------------------------------------------------------------------------
# Python virtualenv
# ---------------------------------------------------------------------------

## Create (or refresh) the Python virtualenv when requirements.txt changes.
$(VENV_MARKER): scripts/requirements.txt
	@printf '\n==> Setting up Python virtualenv in $(VENV) …\n'
	$(PYTHON) -m venv $(VENV)
	$(VENV_BIN)/pip install -q --upgrade pip
	$(VENV_BIN)/pip install -q -r scripts/requirements.txt
	touch $(VENV_MARKER)
	@printf '==> Virtualenv ready.\n'

# Convenience alias
venv: $(VENV_MARKER)

# ---------------------------------------------------------------------------
# Event production
# ---------------------------------------------------------------------------

## Produce N fake clickstream events to the input topic.
## Override defaults:  make produce N=500 CHECKOUT_RATIO=0.8 LATE_RATIO=0.4
produce: $(VENV_MARKER)
	@printf '\n==> Producing $(N) events to [$(INPUT_TOPIC)] on $(HOST_KAFKA) …\n\n'
	$(VENV_BIN)/python scripts/producer.py \
		--bootstrap-servers  $(HOST_KAFKA) \
		--topic              $(INPUT_TOPIC) \
		--num-events         $(N) \
		--checkout-ratio     $(CHECKOUT_RATIO) \
		--late-ratio         $(LATE_RATIO) \
		--max-late-secs      $(MAX_LATE_SECS)

bp: $(VENV_MARKER)
	@printf '\n==> Producing events to [$(INPUT_TOPIC)] on $(HOST_KAFKA) in batches …\n\n'
	$(VENV_BIN)/python scripts/batch_producer.py \
		--bootstrap-servers  $(HOST_KAFKA) \
		--topic              $(INPUT_TOPIC) \

# ---------------------------------------------------------------------------
# Output consumption
# ---------------------------------------------------------------------------

## Tail the checkout-session output topic (press Ctrl-C to exit).
## Prints key | JSON-value for each emitted CheckoutSession.
consume:
	@printf '\n==> Consuming from [$(OUTPUT_TOPIC)] … (Ctrl-C to exit)\n\n'
	podman exec -it clickstream-kafka \
		$(KAFKA_BIN)/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(OUTPUT_TOPIC) \
		--from-beginning \
		--property print.key=true \
		--property key.separator=" | " \
		--property print.timestamp=true

## Tail the price-stats topic (press Ctrl-C to exit).
## Prints a JSON record per 5-minute window with min, max, and average checkout prices.
consume-price-stats:
	@printf '\n==> Consuming from [$(PRICE_STATS_TOPIC)] … (Ctrl-C to exit)\n\n'
	podman exec -it clickstream-kafka \
		$(KAFKA_BIN)/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic $(PRICE_STATS_TOPIC) \
		--from-beginning \
		--property print.key=true \
		--property key.separator=" | " \
		--property print.timestamp=true

# ---------------------------------------------------------------------------
# Flink job management
# ---------------------------------------------------------------------------

## Upload the fat JAR to Flink and start the job.
## The job reads Kafka config from env vars set in podman-compose.yml.
submit-job: $(JAR) cancel-jobs
	@printf '\n==> Uploading $(JAR) to Flink REST API …\n'
	@JAR_ID=$$(curl -sf -X POST -H "Expect:" \
		-F "jarfile=@$(JAR)" \
		$(FLINK_REST)/jars/upload \
		| $(PYTHON) -c \
		  "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])") \
	&& printf '==> JAR uploaded  : %s\n' "$$JAR_ID" \
	&& printf '==> Submitting job …\n' \
	&& JOB_RESPONSE=$$(curl -sf -X POST \
		"$(FLINK_REST)/jars/$$JAR_ID/run" \
		-H "Content-Type: application/json" \
		-d "{\"entryClass\":\"com.example.clickstream.ClickStreamJob\",\"parallelism\":1}") \
	&& printf '==> Job response  : %s\n' "$$JOB_RESPONSE" \
	&& printf '\n==> Job submitted! Watch progress at $(FLINK_REST)\n'

## List all running Flink jobs.
list-jobs:
	@printf '\n==> Running Flink jobs:\n'
	@curl -sf $(FLINK_REST)/jobs | \
		$(PYTHON) -c \
		"import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); \
		 [print(f\"  {j['id']}  status={j['status']}\") for j in jobs] \
		 or print('  (none)')"

## Cancel all RUNNING Flink jobs.
cancel-jobs:
	@printf '\n==> Cancelling all running jobs …\n'
	@curl -sf $(FLINK_REST)/jobs | \
		$(PYTHON) -c \
		"import sys,json,subprocess; \
		 jobs=json.load(sys.stdin).get('jobs',[]); \
		 running=[j['id'] for j in jobs if j['status']=='RUNNING']; \
		 [subprocess.run(['curl','-sf','-X','PATCH', \
		   '$(FLINK_REST)/jobs/'+jid+'?mode=cancel']) for jid in running]; \
		 print(f'Cancelled {len(running)} job(s).')"

## Trigger a savepoint on the running job and stop it gracefully.
## The savepoint path is written to $(SAVEPOINT_PATH_FILE) for use by
## restart-from-savepoint.  Override the savepoint directory with
##   make savepoint-stop SAVEPOINT_DIR=/my/dir
savepoint-stop:
	@printf '\n==> Creating savepoint and stopping job …\n'
	@set -e; \
	JOB_ID=$$(curl -sf $(FLINK_REST)/jobs | \
		$(PYTHON) -c \
		"import sys,json; \
		 jobs=json.load(sys.stdin).get('jobs',[]); \
		 running=[j['id'] for j in jobs if j['status']=='RUNNING']; \
		 print(running[0]) if running else (print('ERROR: no RUNNING job found',file=__import__('sys').stderr) or __import__('sys').exit(1))"); \
	printf '==> Job ID        : %s\n' "$$JOB_ID"; \
	REQ_ID=$$(curl -sf -X POST \
		"$(FLINK_REST)/jobs/$$JOB_ID/stop" \
		-H "Content-Type: application/json" \
		-d "{\"drain\":false,\"targetDirectory\":\"file://$(SAVEPOINT_DIR)\"}" \
		| $(PYTHON) -c "import sys,json; print(json.load(sys.stdin)['request-id'])"); \
	printf '==> Request ID    : %s\n' "$$REQ_ID"; \
	printf '==> Waiting for savepoint '; \
	for i in $$(seq 1 40); do \
		SP_STATUS=$$(curl -sf \
			"$(FLINK_REST)/jobs/$$JOB_ID/savepoints/$$REQ_ID" | \
			$(PYTHON) -c \
			"import sys,json; \
			 d=json.load(sys.stdin); \
			 s=d['status']['id']; \
			 loc=(d.get('operation') or {}).get('location',''); \
			 print(s + (' ' + loc if loc else ''))"); \
		STATUS_ID=$$(printf '%s' "$$SP_STATUS" | cut -d' ' -f1); \
		if [ "$$STATUS_ID" = "COMPLETED" ]; then \
			SP_PATH=$$(printf '%s' "$$SP_STATUS" | cut -d' ' -f2-); \
			printf ' ✓\n'; \
			printf '==> Savepoint at  : %s\n' "$$SP_PATH"; \
			printf '%s\n' "$$SP_PATH" > $(SAVEPOINT_PATH_FILE); \
			printf '==> Path written to $(SAVEPOINT_PATH_FILE)\n'; \
			printf '\n==> Job stopped. Resume with: make restart-from-savepoint\n'; \
			exit 0; \
		elif [ "$$STATUS_ID" = "FAILED" ]; then \
			printf '\nERROR: savepoint failed.\n'; exit 1; \
		fi; \
		printf '.'; sleep 3; \
	done; \
	printf '\nERROR: savepoint did not complete in time.\n'; exit 1

## Upload the fat JAR and restart the job from the last savepoint.
## The savepoint path is read from $(SAVEPOINT_PATH_FILE) (written by savepoint-stop).
## You can override it directly: make restart-from-savepoint SAVEPOINT_PATH_FILE=my-path-file
restart-from-savepoint: $(JAR)
	@printf '\n==> Restarting job from savepoint …\n'
	@test -f $(SAVEPOINT_PATH_FILE) || \
		{ printf 'ERROR: $(SAVEPOINT_PATH_FILE) not found – run "make savepoint-stop" first.\n'; exit 1; }
	@set -e; \
	SP_PATH=$$(cat $(SAVEPOINT_PATH_FILE)); \
	printf '==> Savepoint     : %s\n' "$$SP_PATH"; \
	printf '==> Uploading $(JAR) …\n'; \
	JAR_ID=$$(curl -sf -X POST -H "Expect:" \
		-F "jarfile=@$(JAR)" \
		$(FLINK_REST)/jars/upload \
		| $(PYTHON) -c \
		  "import sys,json; print(json.load(sys.stdin)['filename'].split('/')[-1])"); \
	printf '==> JAR uploaded  : %s\n' "$$JAR_ID"; \
	printf '==> Submitting job …\n'; \
	JOB_RESPONSE=$$(curl -sf -X POST \
		"$(FLINK_REST)/jars/$$JAR_ID/run" \
		-H "Content-Type: application/json" \
		-d "{\"entryClass\":\"com.example.clickstream.ClickStreamJob\", \
		     \"parallelism\":2, \
		     \"savepointPath\":\"$$SP_PATH\", \
		     \"allowNonRestoredState\":false}"); \
	printf '==> Job response  : %s\n' "$$JOB_RESPONSE"; \
	printf '\n==> Job restarted from savepoint! Watch progress at $(FLINK_REST)\n'

# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

## Stop and prune only the Flink JobManager and TaskManager containers.
stop-flink:
	@printf '\n==> Stopping Flink containers …\n'
	-$(COMPOSE) stop taskmanager jobmanager
	-podman rm clickstream-taskmanager
	-podman rm clickstream-jobmanager
	@printf 'Done.\n'

start-flink:
	@printf '\n==> Starting Flink containers …\n'
	-$(COMPOSE) up -d jobmanager taskmanager
	@printf 'Done.\n'

## Tail Flink JobManager logs.
logs-jm:
	podman logs -f clickstream-jobmanager

## Tail Flink TaskManager logs.
logs-tm:
	podman logs -f clickstream-taskmanager

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Interactive shells
# ---------------------------------------------------------------------------

## Open a bash shell inside the Kafka broker container.
shell-kafka:
	podman exec -it clickstream-kafka /bin/bash

## Open a shell inside the Kafka UI container.
shell-kafka-ui:
	podman exec -it clickstream-kafka-ui /bin/sh

## Open a bash shell inside the Flink JobManager container.
shell-jm:
	podman exec -it clickstream-jobmanager /bin/bash

## Open a bash shell inside the Flink TaskManager container.
shell-tm:
	podman exec -it clickstream-taskmanager /bin/bash

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

## Remove Maven build artefacts.
clean:
	mvn clean
	rm -rf $(VENV)
	@printf 'Build artefacts removed.\n'
