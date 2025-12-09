paas-kafka-code
================

This repository contains the Python code for the Pizza-as-a-Service Kafka edition.

- Shared application package: pizza_app/
- Chapter-specific entrypoints: chapter10/, chapter11/, ...

Quick start:

    python -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

Dry-run event generation (no Kafka):

    python -m chapter10.dry_run_event_models

Produce events to a local Kafka running on localhost:9092:

    export RUN_MODE=kafka
    python -m chapter11.produce_orders_local_kafka

Consume events from the incoming-orders topic:

    python -m chapter11.consume_orders_local_kafka



# Chapter 11
## CLI: Install + Run Kafka in KRaft (Ubuntu 24.04, single node)

# --- System prep ---
sudo apt update
sudo apt upgrade -y

# Basic tools
sudo apt install -y wget curl tar unzip

# Java (OpenJDK 21 is fine for Kafka 3.7)
sudo apt install -y openjdk-21-jdk
java --version
