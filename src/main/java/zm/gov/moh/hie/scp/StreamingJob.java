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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
                .map(new StringToLabOrderMapFunction(cfg.jdbcUrl, cfg.jdbcUser, cfg.jdbcPassword))
                .filter(order -> order != null)
                .name("Parse HL7 Messages").disableChaining();

        final var jdbcSink = JdbcSink.getLabOrderSinkFunction(cfg.jdbcUrl, cfg.jdbcUser, cfg.jdbcPassword);

        orders.addSink(jdbcSink).name("Postgres JDBC -> Lab Order Sink");

        // Execute the pipeline
        env.execute("Kafka to Postgres SC / Disa Lab Orders Pipeline");
    }

    private static class StringToLabOrderMapFunction extends RichMapFunction<String, LabOrder> {
        private static final Logger LOG = LoggerFactory.getLogger(StringToLabOrderMapFunction.class);
        private final String jdbcUrl;
        private final String jdbcUser;
        private final String jdbcPassword;
        private transient Connection dbConnection;

        public StringToLabOrderMapFunction(String jdbcUrl, String jdbcUser, String jdbcPassword) {
            this.jdbcUrl = jdbcUrl;
            this.jdbcUser = jdbcUser;
            this.jdbcPassword = jdbcPassword;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                dbConnection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
                LOG.info("Connected to database for LOINC lookup");
            } catch (Exception e) {
                LOG.error("Failed to establish database connection for LOINC lookup: {}", e.getMessage(), e);
            }
        }

        @Override
        public void close() throws Exception {
            if (dbConnection != null && !dbConnection.isClosed()) {
                dbConnection.close();
                LOG.info("Closed database connection");
            }
            super.close();
        }

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
                Short testId = null;

                // Extract order date/time from message
                String[] dateTimeParts = extractDateTimeFromMessage(omlString);
                if (dateTimeParts != null) {
                    orderDate = dateTimeParts[0];
                    orderTime = dateTimeParts[1];
                }

                // Extract LOINC code from OBR-4 (Universal Service ID) and lookup test_id
                String loincCode = extractLoincFromMessage(omlString);
                if (loincCode != null && !loincCode.isEmpty()) {
                    LOG.debug("Extracted LOINC code: {}", loincCode);
                    // Lookup test_id from database using LOINC code
                    testId = lookupTestIdByLoinc(loincCode);
                    if (testId != null) {
                        LOG.debug("Found test_id {} for LOINC code {}", testId, loincCode);
                    } else {
                        LOG.warn("No test_id found in database for LOINC code: {}", loincCode);
                    }
                } else {
                    LOG.debug("No LOINC code found in message");
                }

                // Create LabOrder object
                if (orderId != null && header != null && header.getMessageId() != null) {
                    LabOrder labOrder = new LabOrder(
                            header,
                            hmisCode != null ? hmisCode : "",
                            orderId,
                            testId,
                            orderDate,
                            orderTime,
                            header.getMessageId(),
                            sendingApplication != null ? sendingApplication : ""
                    );
                    LOG.info("Parsed LabOrder: orderId={}, messageRefId={}, loincCode={}, testId={}",
                            orderId, header.getMessageId(), loincCode, testId);
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

        private Short lookupTestIdByLoinc(String loincCode) {
            if (dbConnection == null || loincCode == null || loincCode.isEmpty()) {
                return null;
            }
            try {
                String sql = "SELECT test_id FROM ref.lab_test WHERE loinc = ? LIMIT 1";
                try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
                    stmt.setString(1, loincCode);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            return rs.getShort("test_id");
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error looking up test_id for LOINC code {}: {}", loincCode, e.getMessage());
            }
            return null;
        }

        private String extractLoincFromMessage(String message) {
            // Extract LOINC code from OBR-4 (Universal Service ID)
            // OBR segment format: OBR|field1|field2|field3|field4|...
            // OBR-4 can contain: identifier^text^name_of_coding_system^alt_id^alt_text^alt_name_of_coding_system
            // LOINC is typically in the alternate identifier position
            String[] lines = message.split("\r");
            for (String line : lines) {
                if (line.startsWith("OBR|")) {
                    String[] fields = line.split("\\|");
                    if (fields.length > 4 && !fields[4].isEmpty()) {
                        try {
                            String obrField = fields[4];
                            // Split by ^ to get components
                            String[] components = obrField.split("\\^");
                            // Try to find LOINC code (usually in position 0 or 3)
                            if (components.length > 0 && !components[0].isEmpty()) {
                                return components[0]; // Return main identifier
                            }
                            if (components.length > 3 && !components[3].isEmpty()) {
                                return components[3]; // Return alternate identifier (LOINC)
                            }
                        } catch (Exception e) {
                            LOG.debug("Could not extract LOINC from OBR-4: {}", fields[4]);
                        }
                    }
                }
            }
            return null;
        }
    }
}
