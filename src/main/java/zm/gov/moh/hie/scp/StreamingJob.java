package zm.gov.moh.hie.scp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.deserializer.LabOrderDeserializer;
import zm.gov.moh.hie.scp.dto.LabOrder;
import zm.gov.moh.hie.scp.sink.DeferredJdbcSink;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    private static java.util.Properties buildProducerConfig(Config cfg) {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("bootstrap.servers", cfg.kafkaBootstrapServers);
        props.setProperty("security.protocol", cfg.kafkaSecurityProtocol);
        props.setProperty("sasl.mechanism", cfg.kafkaSaslMechanism);
        props.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + cfg.kafkaSaslUsername + "\" " +
                        "password=\"" + cfg.kafkaSaslPassword + "\";");
        return props;
    }

    public static void main(String[] args) throws Exception {
        // Load effective configuration (CLI > env > defaults)
        final Config cfg = Config.fromEnvAndArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create Kafka source with LabOrderDeserializer
        KafkaSource<LabOrder> source = KafkaSource.<LabOrder>builder()
                .setBootstrapServers(cfg.kafkaBootstrapServers)
                .setTopics(cfg.kafkaTopic)
                .setGroupId(cfg.kafkaGroupId)
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "2000")
                .setProperty("max.poll.interval.ms", "10000")
                .setProperty("max.poll.records", "50")
                .setProperty("request.timeout.ms", "2540000")
                .setProperty("delivery.timeout.ms", "120000")
                .setProperty("default.api.timeout.ms", "2540000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new LabOrderDeserializer())
                .setProperty("security.protocol", cfg.kafkaSecurityProtocol)
                .setProperty("sasl.mechanism", cfg.kafkaSaslMechanism)
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                                "username=\"" + cfg.kafkaSaslUsername + "\" " +
                                "password=\"" + cfg.kafkaSaslPassword + "\";")
                .build();

        // Create DataStream from Kafka source
        DataStream<LabOrder> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        ).startNewChain();

        // Filter null orders and log
        DataStream<LabOrder> filteredStream = kafkaStream
                .filter(order -> {
                    if (order == null) {
                        LOG.debug("Received null LabOrder");
                        return false;
                    }
                    if (order.getMessageRefId() == null || order.getMessageRefId().isEmpty()) {
                        LOG.warn("Received LabOrder with empty messageRefId");
                        return false;
                    }
                    LOG.info("Processing LabOrder: orderId={}, messageRefId={}", order.getOrderId(), order.getMessageRefId());
                    return true;
                })
                .name("Filter Null Values").disableChaining();

        // Filter messages with non-MFL facility codes (accept 4 digit MFL codes, reject HMIS codes)
        DataStream<LabOrder> mflFilteredStream = filteredStream
                .filter(order -> {
                    String mflCode = order.getMflCode();
                    if (mflCode == null || mflCode.isEmpty() || mflCode.length() != 4) {
                        LOG.warn("Filtered out LabOrder with invalid MFL code. mflCode='{}' (length={}), messageRefId={}",
                                mflCode, mflCode != null ? mflCode.length() : "null", order.getMessageRefId());
                        return false;
                    }
                    return true;
                })
                .name("Filter Non-MFL Codes").disableChaining();

        // Create JDBC Sink
        // Using deferred sink to avoid DNS resolution errors during operator initialization
        final String upsertSql = "INSERT INTO crt.lab_order (order_id, message_ref_id, mfl_code, order_date, order_time, sending_application, test_id, lab_code) " +
                "VALUES (?, ?, ?, ?::date, ?::time, ?, ?, ?) " +
                "ON CONFLICT (message_ref_id) DO UPDATE SET " +
                "order_id = EXCLUDED.order_id, " +
                "mfl_code = EXCLUDED.mfl_code, " +
                "order_date = EXCLUDED.order_date, " +
                "order_time = EXCLUDED.order_time, " +
                "sending_application = EXCLUDED.sending_application, " +
                "test_id = EXCLUDED.test_id, " +
                "lab_code = EXCLUDED.lab_code";

        // Split stream: valid orders (with lab_code) vs DLQ (missing lab_code)
        DataStream<LabOrder> validStream = mflFilteredStream
                .filter(o -> o.getLabCode() != null && !o.getLabCode().isEmpty())
                .name("Filter: Has Lab Code");

        DataStream<LabOrder> dlqStream = mflFilteredStream
                .filter(o -> o.getLabCode() == null || o.getLabCode().isEmpty())
                .name("Filter: Missing Lab Code -> DLQ");

        // Route valid orders to JDBC sink
        validStream.addSink(new DeferredJdbcSink(
                cfg.jdbcUrl,
                cfg.jdbcUser,
                cfg.jdbcPassword,
                upsertSql,
                1000,      // batchSize
                200,       // batchIntervalMs
                5          // maxRetries
        )).name("Postgres JDBC -> Lab Order Sink");

        // Route DLQ orders to Kafka topic lab-orders-dlq
        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(cfg.kafkaBootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("lab-orders-dlq")
                                .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(buildProducerConfig(cfg))
                .build();

        dlqStream
                .map(o -> o.getRawMessage())
                .sinkTo(dlqSink)
                .name("Kafka DLQ -> lab-orders-dlq");

        // Execute the pipeline
        env.execute("Kafka to Postgres SC / Disa Lab Orders Pipeline");
    }
}
