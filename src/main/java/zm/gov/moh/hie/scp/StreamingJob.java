package zm.gov.moh.hie.scp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.deserializer.LabOrderDeserializer;
import zm.gov.moh.hie.scp.dto.LabOrder;
import zm.gov.moh.hie.scp.sink.JdbcSink;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        // Load effective configuration (CLI > env > defaults)
        final Config cfg = Config.fromEnvAndArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        // Create a DataStream from a Kafka source
        DataStream<LabOrder> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        ).startNewChain();

        // Filter out null values to prevent issues with the sink
        DataStream<LabOrder> filteredStream = kafkaStream
                .filter(order -> {
                    if (order == null) {
                        LOG.warn("Filtered out null LabOrder");
                        return false;
                    }
                    if (order.getHeader() != null) {
                        LOG.info("Processing LabOrder with message ID: {}", order.getHeader().getMessageId());
                    } else {
                        LOG.info("Processing LabOrder with no header");
                    }
                    return true;
                })
                .name("Filter Null Values").disableChaining();

        final var jdbcSink = JdbcSink.getLabOrderSinkFunction(cfg.jdbcUrl, cfg.jdbcUser, cfg.jdbcPassword);

        filteredStream.addSink(jdbcSink).name("Postgres JDBC -> Lab Order Sink");

        // Execute the pipeline
        env.execute("Kafka to Postgres SC / Disa Lab Orders Pipeline");
    }
}
