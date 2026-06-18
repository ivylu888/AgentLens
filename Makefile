# =====================================================================
# AgentLens Data Pipeline Automation Controller (Makefile)
# =====================================================================

.PHONY: init up down run-bronze run-silver run-gold logs

# 1. Bootstrapping: Environment Provisioning & Static Jar Caching
init:
	@echo "Installing Python dependencies within local virtualenv..."
	pip install -r requirements.txt
	@echo "Downloading immutable Java jar dependencies into local execution boundary..."
	mkdir -p ./jars
	curl -sL -o ./jars/delta-spark_2.12-3.1.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar
	curl -sL -o ./jars/delta-storage-3.1.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar
	curl -sL -o ./jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
	curl -sL -o ./jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
	@echo " Environment fully provisioned! Please verify your local .env is updated with cloud tokens."

# 2. Infrastructure Coordination (Docker Layer)
up:
	docker-compose up -d
	@echo "Waiting for Prefect control plane to attain stability..."
	@sleep 5
	prefect profile use local-dev

down:
	docker-compose down

# 3. Medallion Architecture Pipeline Ingestion Stages
run-bronze:
	python src/pipelines/bronze/ingest_bronze.py

run-silver:
	python src/pipelines/silver/process_silver.py

run-gold:
	python src/pipelines/gold/process_gold.py

# 📊 4. Observability & Diagnostics
logs:
	docker-compose logs -f prefect-server