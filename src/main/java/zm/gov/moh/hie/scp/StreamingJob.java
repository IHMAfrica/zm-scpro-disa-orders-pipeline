package zm.gov.moh.hie.scp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ca.uhn.hl7v2.model.v25.message.OML_O21;
import zm.gov.moh.hie.scp.dto.Header;
import zm.gov.moh.hie.scp.dto.LabOrder;
import zm.gov.moh.hie.scp.sink.JdbcSink;
import zm.gov.moh.hie.scp.util.DateTimeUtil;
import zm.gov.moh.hie.scp.util.Hl7Parser;

import java.time.LocalDateTime;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        // Load effective configuration (CLI > env > defaults)
        final Config cfg = Config.fromEnvAndArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
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
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", cfg.kafkaSecurityProtocol)
                .setProperty("sasl.mechanism", cfg.kafkaSaslMechanism)
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                                "username=\"" + cfg.kafkaSaslUsername + "\" " +
                                "password=\"" + cfg.kafkaSaslPassword + "\";")
                .build();

        // Create a DataStream from a Kafka source
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        ).startNewChain();

        // Parse HL7 messages and filter
        SingleOutputStreamOperator<LabOrder> orders = kafkaStream
                .filter(msg -> !StringUtils.isNullOrWhitespaceOnly(msg))
                .map(new StringToLabOrderMapFunction())
                .filter(order -> order != null)
                .name("Parse HL7 Messages").disableChaining();

        final var jdbcSink = JdbcSink.getLabOrderSinkFunction(cfg.jdbcUrl, cfg.jdbcUser, cfg.jdbcPassword);

        orders.addSink(jdbcSink).name("Postgres JDBC -> Lab Order Sink");

        // Execute the pipeline
        env.execute("Kafka to Postgres SC / Disa Lab Orders Pipeline");
    }

    private static class StringToLabOrderMapFunction extends RichMapFunction<String, LabOrder> {
        private static final Logger LOG = LoggerFactory.getLogger(StringToLabOrderMapFunction.class);

        @Override
        public LabOrder map(String omlString) {
            try {
                OML_O21 omlMsg = Hl7Parser.toOml021Message(sanitize(omlString));

                if (omlMsg == null) {
                    LOG.debug("Failed to parse OML message");
                    return null;
                }

                Header header = Hl7Parser.toHeader(omlMsg.getMSH());

                if (header == null) {
                    LOG.debug("Failed to extract header from OML message");
                    return null;
                }

                // Extract sending application from MSH-3
                String sendingApplication = null;
                try {
                    sendingApplication = omlMsg.getMSH().getSendingApplication().getNamespaceID().getValue();
                } catch (Exception e) {
                    LOG.debug("Could not extract sending application from MSH-3");
                }

                // Extract HMIS code (facility) from MSH-4 Universal ID
                String hmisCode = null;
                try {
                    hmisCode = omlMsg.getMSH().getSendingFacility().getUniversalID().getValue();
                } catch (Exception e) {
                    LOG.debug("Could not extract HMIS code from MSH-4");
                }

                // Parse order information from message segments
                String orderId = extractOrderIdFromMessage(omlString);
                String orderDate = null;
                String orderTime = null;

                // Extract order date/time from message
                String[] dateTimeParts = extractDateTimeFromMessage(omlString);
                if (dateTimeParts != null) {
                    orderDate = dateTimeParts[0];
                    orderTime = dateTimeParts[1];
                }

                // Create LabOrder object
                if (orderId != null && header != null && header.getMessageId() != null) {
                    LabOrder labOrder = new LabOrder(
                            header,
                            hmisCode != null ? hmisCode : "",
                            orderId,
                            null,
                            orderDate,
                            orderTime,
                            header.getMessageId(),
                            sendingApplication != null ? sendingApplication : ""
                    );
                    LOG.info("Parsed LabOrder: orderId={}, messageRefId={}", orderId, header.getMessageId());
                    return labOrder;
                }

                return null;
            } catch (Exception e) {
                LOG.error("Error deserializing LabOrder: {}", e.getMessage(), e);
                return null;
            }
        }

        private String sanitize(String input) {
            input = input.replace("\n", "\r");
            input = input.replace("\r\r", "\r");
            input = input.replaceAll("(?m)^NTE\\|.*(?:\\r?\\n)?", "");
            input = input.replace("\r\r", "\r");
            return input;
        }

        private String extractOrderIdFromMessage(String message) {
            // Extract order ID from ORC-2 (Placer Order Number)
            // ORC segment format: ORC|field1|field2|...
            String[] lines = message.split("\r");
            for (String line : lines) {
                if (line.startsWith("ORC|")) {
                    String[] fields = line.split("\\|");
                    if (fields.length > 2 && !fields[2].isEmpty()) {
                        // ORC-2 is the placer order number
                        return fields[2].split("\\^")[0]; // Get the main part before any subcomponents
                    }
                }
            }
            return null;
        }

        private String[] extractDateTimeFromMessage(String message) {
            // Extract date/time from OBR-7 (Observation Date/Time)
            // OBR segment format: OBR|field1|field2|...|field7|...
            String[] lines = message.split("\r");
            for (String line : lines) {
                if (line.startsWith("OBR|")) {
                    String[] fields = line.split("\\|");
                    if (fields.length > 7 && !fields[7].isEmpty()) {
                        try {
                            String dateTimeStr = fields[7];
                            // Parse YYYYMMDDHHMM format
                            if (dateTimeStr.length() >= 12) {
                                LocalDateTime dt = LocalDateTime.parse(dateTimeStr.substring(0, 12) + "00", DateTimeUtil.DATETIME_DISA_FORMATTER);
                                return new String[]{
                                        dt.format(DateTimeUtil.DATE_FORMATTER),
                                        dt.format(DateTimeUtil.TIME_FORMATTER)
                                };
                            }
                        } catch (Exception e) {
                            LOG.debug("Could not parse date/time: {}", fields[7]);
                        }
                    }
                }
            }
            return null;
        }
    }
}
