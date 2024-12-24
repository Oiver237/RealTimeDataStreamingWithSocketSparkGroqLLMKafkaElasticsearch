# RealtimeStreaming Project using TCP Socket, Apache Spark running on Docker, Groq LLM, Confluent Cloud and Elasticsearch 

## Overview
This project is designed to process streaming data using Apache Spark and perform sentiment analysis on the incoming data. The analysis leverages Groq's models for classifying comments into sentiments: **POSITIVE**, **NEGATIVE**, or **NEUTRAL**. The results are then sent to a Kafka topic then to an Elasticsearch cluster for downstream processing.


## Architecture

![alt text](assets/System_architecture.png)


## Prerequisites
- Python 3.10+
- Docker
- Groq ai account and a valid API key
- Confluent Kafka for cloud-based Kafka solutions
- Elasticsearch cloud account

## Setup
### Step 1: Clone the Repository
```bash
git clone https://github.com/Oiver237/RealTimeDataStreamingWithSocketSparkGroqLLMKafkaElasticsearch.git
```

### Step 2: Set Up the Virtual Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```
```bash
docker compose up -d --build
```
```bash
python jobs/streaming-socket.py
```
```bash
python jobs/spark-streaming.py
```

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.


