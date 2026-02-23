package zm.gov.moh.hie.scp.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.LabOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * Custom JDBC sink that defers connection until first record arrives.
 * Includes batch execution with configurable batch size and interval.
 * This avoids DNS resolution errors during operator initialization in Kubernetes.
 */
public class DeferredJdbcSink extends RichSinkFunction<LabOrder> {
    private static final Logger LOG = LoggerFactory.getLogger(DeferredJdbcSink.class);

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String upsertSql;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;

    private Connection connection;
    private boolean initialized = false;
    private List<LabOrder> batch;
    private long lastFlushTime;

    public DeferredJdbcSink(String jdbcUrl, String jdbcUser, String jdbcPassword, String upsertSql,
                            int batchSize, long batchIntervalMs, int maxRetries) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.upsertSql = upsertSql;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        // Don't connect here - defer until first record
        this.batch = new ArrayList<>(batchSize);
        this.lastFlushTime = System.currentTimeMillis();
        LOG.info("DeferredJdbcSink opened with batchSize={}, batchIntervalMs={}, maxRetries={}",
                batchSize, batchIntervalMs, maxRetries);
    }

    @Override
    public void invoke(LabOrder element, Context context) throws Exception {
        // Lazy initialization on first record
        if (!initialized) {
            try {
                establishConnection();
                initialized = true;
                LOG.info("JDBC connection established on first record");
            } catch (Exception e) {
                LOG.error("Failed to establish JDBC connection", e);
                throw e;
            }
        }

        // Add to batch
        batch.add(element);

        // Check if we should flush
        boolean shouldFlush = batch.size() >= batchSize ||
                (System.currentTimeMillis() - lastFlushTime) >= batchIntervalMs;

        if (shouldFlush) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        // Flush remaining records
        if (!batch.isEmpty()) {
            flush();
        }
        // Close connection
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("JDBC connection closed");
        }
    }

    private void flush() throws Exception {
        if (batch.isEmpty()) {
            return;
        }

        int retries = 0;
        while (retries < maxRetries) {
            try {
                executeBatch();
                batch.clear();
                lastFlushTime = System.currentTimeMillis();
                LOG.debug("Flushed {} records to database", batch.size());
                return;
            } catch (Exception e) {
                retries++;
                if (retries >= maxRetries) {
                    LOG.error("Failed to execute batch after {} retries", maxRetries, e);
                    throw e;
                }
                LOG.warn("Batch execution failed, retry {} of {}", retries, maxRetries);
                try {
                    Thread.sleep(100); // Brief delay before retry
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new Exception("Interrupted during batch retry", ie);
                }
            }
        }
    }

    private void executeBatch() throws Exception {
        try (PreparedStatement ps = connection.prepareStatement(upsertSql)) {
            for (LabOrder element : batch) {
                ps.setString(1, element.getOrderId());
                ps.setString(2, element.getMessageRefId());
                ps.setString(3, element.getMflCode());
                ps.setString(4, element.getOrderDate());
                ps.setString(5, element.getOrderTime());
                ps.setString(6, element.getSendingApplication());
                ps.setString(7, element.getLoinc());
                ps.setString(8, element.getLabCode());
                ps.addBatch();
            }
            int[] result = ps.executeBatch();
            LOG.info("Executed batch of {} records, affected rows: {}", batch.size(), result.length);
        }
    }

    private void establishConnection() throws Exception {
        Class.forName("org.postgresql.Driver");
        connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
        LOG.info("Connected to database: {}", jdbcUrl);
    }
}
