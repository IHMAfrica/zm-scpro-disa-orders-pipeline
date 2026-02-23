# SCPro DISA Orders Pipeline

An Apache Flink streaming job that consumes lab order messages from Kafka and writes parsed orders to PostgreSQL. The job is packaged as a fat jar and designed to run on a Flink session cluster managed by the Flink Kubernetes Operator. CI builds the fat jar and publishes it as a standalone OCI artifact to GHCR. The FlinkSessionJob uses an initContainer that pulls the jar image from GHCR and copies the jar into Flink's usrlib directory at runtime.

## Features
- Kafka -> Flink -> PostgreSQL streaming pipeline
- Externalized configuration via environment variables and/or command-line arguments
- Deployable via Flink Kubernetes Operator (FlinkSessionJob)
- GitHub Actions pipeline builds the JAR and publishes it as a GHCR OCI artifact; the runtime uses the official Flink image

## Build
Requirements:
- JDK 17
- Gradle Wrapper included

Build fat jar:
- ./gradlew clean shadowJar

Run locally (example):
- export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
- export KAFKA_TOPIC=lab-orders
- export KAFKA_GROUP_ID=flink-local
- export JDBC_URL=jdbc:postgresql://localhost:5432/hie_manager
- export JDBC_USER=postgres
- export JDBC_PASSWORD=postgres
- java -jar build/libs/zm-scpro-disa-orders-pipeline-*-all.jar --kafka.security.protocol=SASL_PLAINTEXT --kafka.sasl.mechanism=SCRAM-SHA-256 --kafka.sasl.username=admin --kafka.sasl.password=secret

Note: When running locally outside a Flink cluster, you might also use the application plugin:
- ./gradlew run --args="--kafka.bootstrap.servers=localhost:9092 --kafka.topic=lab-orders"

## Configuration
Configuration can be provided via environment variables or command-line arguments. Precedence: CLI args override environment variables; environment variables override built-in defaults. Secrets should be provided via environment variables or Kubernetes secrets, not hard-coded.

Supported keys (CLI args use --key=value form):
- kafka.bootstrap.servers / KAFKA_BOOTSTRAP_SERVERS
- kafka.topic / KAFKA_TOPIC
- kafka.group.id / KAFKA_GROUP_ID
- kafka.security.protocol / KAFKA_SECURITY_PROTOCOL (e.g., SASL_PLAINTEXT or SASL_SSL)
- kafka.sasl.mechanism / KAFKA_SASL_MECHANISM (e.g., SCRAM-SHA-256)
- kafka.sasl.username / KAFKA_SASL_USERNAME
- kafka.sasl.password / KAFKA_SASL_PASSWORD
- jdbc.url / JDBC_URL (e.g., jdbc:postgresql://host:5432/db)
- jdbc.user / JDBC_USER
- jdbc.password / JDBC_PASSWORD

Example CLI args:
- --kafka.bootstrap.servers=broker1:9092,broker2:9092
- --kafka.topic=lab-orders
- --jdbc.url=jdbc:postgresql://db:5432/hie_manager

## Architecture

The pipeline:
1. **Source**: Consumes OML^O21 HL7 messages from Kafka topic `lab-orders`
2. **Deserializer**: Parses HL7 messages to extract order data:
   - Order ID from ORC segment
   - Facility code (HMIS) from MSH segment
   - Test information from OBR segment
   - Message control ID for deduplication
3. **Sink**: Writes to PostgreSQL table `crt.lab_order` with upsert logic based on message_ref_id

## Message Flow

Input: HL7 OML^O21 message from Kafka
Output: Structured order record in `crt.lab_order` table with fields:
- hmis_code: Sending facility identifier
- order_id: Placer order number
- test_id: Reference to lab test
- order_date: Order date from OBR segment
- order_time: Order time from OBR segment
- message_ref_id: Unique message identifier
- sending_application: Sending application namespace ID

## Deployment

See `k8s/fleet/` for Kubernetes manifests compatible with Rancher Fleet and the Flink Kubernetes Operator.

Requirements for deployment:
- Flink Kubernetes Operator installed
- Existing `session-cluster` FlinkDeployment in `flink-jobs` namespace
- Kafka connectivity configured
- PostgreSQL database accessible

## Testing

./gradlew test

## License
