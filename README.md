# Kafka Fundamentals and OpenSearch Integration

Overview
- This repository contains example projects and demos for learning Apache Kafka using Java and Maven.
- Main modules:
  - `kafka-basics` — simple producer/consumer demos.
  - `kafka-producer-wikimedia` — a Wikimedia stream producer that reads Server-Sent Events and publishes to Kafka.
  - `kafka-consumer-opensearch` — consumer example for indexing into OpenSearch.
  - `conduktor-platform` — Docker Compose setup for running Kafka stack locally.

Prerequisites
- Java (JDK 11+ recommended)
- Maven
- Docker & Docker Compose (for the local Kafka stack)
- IntelliJ IDEA (project is developed with IntelliJ; optional)

Quick start (local Kafka using Conduktor platform)
1. Start the platform:
   - `docker compose -f conduktor-platform/docker-compose.yaml up -d`
2. Verify Kafka is running (Conduktor UI / Kafka ports).

Build
- From repository root:
  - `mvn clean package`
- To build a single module:
  - `mvn -pl kafka-producer-wikimedia -am clean package`

Run (recommended: use IntelliJ)
- Open the project in IntelliJ and run the desired main class from the module:
  - `kafka-basics` demos: check `src/main/java/io/conduktor/demos/...` in that module.
  - Wikimedia producer: see `kafka-producer-wikimedia/src/main/java/io/conduktor/demos/kafka/wikimedia/` for the event handler and the producer entry point.
- Alternatively run via Maven Exec (if a main class is available):
  - `mvn -pl kafka-producer-wikimedia exec:java -Dexec.mainClass="io.conduktor.demos.kafka.wikimedia.YourMainClass"`

Key files and locations
- `kafka-producer-wikimedia/src/main/java/io/conduktor/demos/kafka/wikimedia/WikimediaChangeHandler.java` — handler that receives SSE events and publishes them to Kafka.
- `kafka-basics/src/main/java/io/conduktor/demos/producer/kafka` — basic producer examples.
- `kafka-basics/src/main/java/io/conduktor/demos/consumer/kafka` — basic consumer examples (including cooperative/shutdown demos).
- `conduktor-platform/docker-compose.yaml` — preconfigured Kafka platform for local testing.

Logging and configuration
- Projects use SLF4J (backed by the logging implementation configured via Maven resources).
- Kafka configuration (bootstrap servers, serializers, topics) is typically set in code or properties per module; adjust before running against your Kafka endpoint.

Troubleshooting
- If consumers/producers cannot connect, ensure Docker Compose stack is up and `bootstrap.servers` in your code points to the correct host/port.
- Check logs in IntelliJ or container logs: `docker compose -f conduktor-platform/docker-compose.yaml logs -f`.

Contributing
- Keep examples small and focused.
- Use Maven modules for isolated builds and easier execution.

