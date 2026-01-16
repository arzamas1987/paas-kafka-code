# Chapter 13 — Docker Kafka Lab (Confluent + KRaft, 3 brokers)

This folder contains **infrastructure as code** (Docker Compose) for Kafka.
The Pizza-as-a-Service application stays **outside Docker** and runs as normal Python
processes (virtual environment), exactly like in Chapter 12.

## 1) One-time: generate the KRaft cluster id

From this folder:

```bash
cd paas-kafka-code/chapter13/docker
