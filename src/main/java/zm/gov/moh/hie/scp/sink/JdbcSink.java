package zm.gov.moh.hie.scp.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.LabOrder;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;

public class JdbcSink {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSink.class);

    public static SinkFunction<LabOrder> getLabOrderSinkFunction(String jdbcUrl, String jdbcUser, String jdbcPassword) {
        // Configure a PostgreSQL JDBC sink for crt.lab_order table
        final String upsertSql = "INSERT INTO crt.lab_order (hmis_code, order_id, test_id, order_date, order_time, message_ref_id, sending_application) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (message_ref_id) DO UPDATE SET " +
                "hmis_code = EXCLUDED.hmis_code, " +
                "order_id = EXCLUDED.order_id, " +
                "test_id = EXCLUDED.test_id, " +
                "order_date = EXCLUDED.order_date, " +
                "order_time = EXCLUDED.order_time, " +
                "sending_application = EXCLUDED.sending_application";

        JdbcStatementBuilder<LabOrder> stmtBuilder = (PreparedStatement ps, LabOrder element) -> {
            // 1. hmis_code
            String hmisCode = element.getHmisCode();
            if (hmisCode == null || hmisCode.isEmpty()) {
                ps.setNull(1, java.sql.Types.VARCHAR);
            } else {
                ps.setString(1, hmisCode);
            }

            // 2. order_id
            String orderId = element.getOrderId();
            if (orderId == null || orderId.isEmpty()) {
                ps.setNull(2, java.sql.Types.VARCHAR);
            } else {
                ps.setString(2, orderId);
            }

            // 3. test_id
            Short testId = element.getTestId();
            if (testId == null) {
                ps.setNull(3, java.sql.Types.SMALLINT);
            } else {
                ps.setShort(3, testId);
            }

            // 4. order_date
            String orderDate = element.getOrderDate();
            if (orderDate == null || orderDate.isEmpty()) {
                ps.setNull(4, java.sql.Types.DATE);
            } else {
                try {
                    ps.setDate(4, Date.valueOf(orderDate));
                } catch (Exception e) {
                    LOG.warn("Failed to parse order date: {}", orderDate);
                    ps.setNull(4, java.sql.Types.DATE);
                }
            }

            // 5. order_time
            String orderTime = element.getOrderTime();
            if (orderTime == null || orderTime.isEmpty()) {
                ps.setNull(5, java.sql.Types.TIME);
            } else {
                try {
                    ps.setTime(5, Time.valueOf(orderTime));
                } catch (Exception e) {
                    LOG.warn("Failed to parse order time: {}", orderTime);
                    ps.setNull(5, java.sql.Types.TIME);
                }
            }

            // 6. message_ref_id
            String messageRefId = element.getMessageRefId();
            if (messageRefId == null || messageRefId.isEmpty()) {
                messageRefId = "unknown-" + System.currentTimeMillis();
            }
            ps.setString(6, messageRefId);

            // 7. sending_application
            String sendingApplication = element.getSendingApplication();
            if (sendingApplication == null || sendingApplication.isEmpty()) {
                ps.setNull(7, java.sql.Types.VARCHAR);
            } else {
                ps.setString(7, sendingApplication);
            }
        };

        final var jdbcSink = org.apache.flink.connector.jdbc.JdbcSink.sink(
                upsertSql,
                stmtBuilder,
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(2000)
                        .withBatchSize(50)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(jdbcUser)
                        .withPassword(jdbcPassword)
                        .build()
        );
        return jdbcSink;
    }
}
