.PHONY: run stop clean logs

DATA_PATH ?= ./data/raw

# Runs the whole pipeline end-to-end (ingestion -> silver -> gold -> dashboard)
# and serves the dashboard on http://localhost:8080
run:
	DATA_PATH=$(DATA_PATH) docker compose up --build

stop:
	docker compose down

# Stops everything and wipes generated data (spark_data volume, minio volume, output/)
clean:
	docker compose down -v
	rm -rf output/*

logs:
	docker compose logs -f pipeline
