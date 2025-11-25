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

    python chapter10/dry_run_event_models.py

Produce events to a local Kafka running on localhost:9092:

    export RUN_MODE=kafka
    python chapter11/produce_orders_local_kafka.py

Consume events from the incoming-orders topic:

    python chapter11/consume_orders_local_kafka.py
