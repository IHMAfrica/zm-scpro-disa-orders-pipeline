package zm.gov.moh.hie.scp.deserializer;

import ca.uhn.hl7v2.model.v25.message.OML_O21;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.LabOrder;
import zm.gov.moh.hie.scp.util.Hl7Parser;

import java.io.IOException;
import java.time.LocalDateTime;

/**
 * Deserializer for OML^O21 HL7 messages from Kafka.
 * Parses lab order messages and extracts relevant fields into LabOrder DTOs.
 */
public class LabOrderDeserializer implements DeserializationSchema<LabOrder> {
    private static final Logger LOG = LoggerFactory.getLogger(LabOrderDeserializer.class);

    @Override
    public LabOrder deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            LOG.warn("Received empty or null message");
            return null;
        }

        try {
            String omlString = new String(message);
            LOG.debug("Received raw message: {}", omlString.substring(0, Math.min(100, omlString.length())));

            OML_O21 omlMsg = Hl7Parser.toOml021Message(sanitize(omlString));

            if (omlMsg == null) {
                LOG.warn("Failed to parse OML message from: {}", omlString.substring(0, Math.min(100, omlString.length())));
                return null;
            }

            // Extract message ID (MSH-10)
            String messageRefId = null;
            try {
                messageRefId = omlMsg.getMSH().getMessageControlID().getValue();
            } catch (Exception e) {
                LOG.debug("Could not extract message control ID from MSH-10");
            }

            if (messageRefId == null || messageRefId.isEmpty()) {
                LOG.debug("Message control ID is empty, skipping message");
                return null;
            }

            // Extract sending application (MSH-3)
            String sendingApplication = null;
            try {
                sendingApplication = omlMsg.getMSH().getSendingApplication().getNamespaceID().getValue();
            } catch (Exception e) {
                LOG.debug("Could not extract sending application from MSH-3");
            }

            // Extract MFL code from MSH-4 with robust fallback strategies
            String mflCode = extractMflCodeRobust(omlMsg, omlString);

            // Extract order ID (ORC-2 Placer Order Number)
            String orderId = extractOrderIdFromMessage(omlString);

            // Extract order date/time (OBR-7)
            String orderDate = null;
            String orderTime = null;
            String[] dateTimeParts = extractDateTimeFromMessage(omlString);
            if (dateTimeParts != null) {
                orderDate = dateTimeParts[0];
                orderTime = dateTimeParts[1];
            }

            // Extract LOINC code from OBR-4 (will be used for test_id lookup in SQL)
            String loinc = extractLoincFromMessage(omlString);
            if (loinc != null) {
                LOG.debug("Extracted LOINC code: {}", loinc);
            }

            // Create LabOrder DTO
            LabOrder labOrder = new LabOrder(
                    null,  // header will be null, not needed for simple upsert
                    mflCode != null ? mflCode : "",
                    orderId,
                    null,  // testId will be looked up in SQL from LOINC code
                    orderDate,
                    orderTime,
                    messageRefId,
                    sendingApplication != null ? sendingApplication : "",
                    loinc  // pass LOINC for SQL lookup
            );

            LOG.info("Successfully deserialized LabOrder: orderId={}, messageRefId={}, mflCode={}, sendingApp={}, orderDate={}, orderTime={}",
                    orderId, messageRefId, mflCode, sendingApplication, orderDate, orderTime);
            return labOrder;

        } catch (Exception e) {
            LOG.error("Error deserializing LabOrder: {}", e.getMessage(), e);
            return null;
        }
    }

    private String sanitize(String input) {
        input = input.replace("\n", "\r");
        input = input.replace("\r\r", "\r");
        input = input.replaceAll("(?m)^NTE\\|.*(?:\r?\n)?", "");
        input = input.replace("\r\r", "\r");
        return input;
    }

    private String extractOrderIdFromMessage(String message) {
        // Extract order ID from ORC-2 (Placer Order Number)
        String[] lines = message.split("\r");
        for (String line : lines) {
            if (line.startsWith("ORC|")) {
                String[] fields = line.split("\\|");
                if (fields.length > 2 && !fields[2].isEmpty()) {
                    return fields[2].split("\\^")[0];
                }
            }
        }
        return null;
    }

    private String[] extractDateTimeFromMessage(String message) {
        // Extract date/time from OBR-7 (Observation Date/Time)
        String[] lines = message.split("\r");
        for (String line : lines) {
            if (line.startsWith("OBR|")) {
                String[] fields = line.split("\\|");
                if (fields.length > 7 && !fields[7].isEmpty()) {
                    try {
                        String dateTimeStr = fields[7];
                        // Parse YYYYMMDDHHMM format
                        if (dateTimeStr.length() >= 12) {
                            LocalDateTime dt = LocalDateTime.parse(dateTimeStr.substring(0, 12) + "00",
                                    zm.gov.moh.hie.scp.util.DateTimeUtil.DATETIME_DISA_FORMATTER);
                            return new String[]{
                                    dt.format(zm.gov.moh.hie.scp.util.DateTimeUtil.DATE_FORMATTER),
                                    dt.format(zm.gov.moh.hie.scp.util.DateTimeUtil.TIME_FORMATTER)
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

    private String extractLoincFromMessage(String message) {
        // Extract LOINC code from OBR-4 (Universal Service ID)
        // OBR-4 format: identifier^text^name_of_coding_system^alt_id^alt_text^alt_name_of_coding_system
        // LOINC code is typically in position 0 or 3 (alternate identifier)
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
                            return components[0];  // Return main identifier
                        }
                        if (components.length > 3 && !components[3].isEmpty()) {
                            return components[3];  // Return alternate identifier (LOINC)
                        }
                    } catch (Exception e) {
                        LOG.debug("Could not extract LOINC from OBR-4: {}", fields[4]);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(LabOrder nextElement) {
        return false;
    }

    @Override
    public TypeInformation<LabOrder> getProducedType() {
        return TypeInformation.of(LabOrder.class);
    }

    /**
     * Extract MFL code from MSH-4 with robust fallback strategies.
     * Handles cases where HAPI parsing may return unexpected values.
     *
     * Tier 1: Try HAPI getUniversalID() (standard approach)
     * Tier 2: Try alternative HAPI accessor (getHd2_UniversalID)
     * Tier 3: Manual string parsing of MSH-4 field (fallback)
     * Tier 4: Try extracting first 4-digit sequence from facility field
     */
    private String extractMflCodeRobust(OML_O21 omlMsg, String rawMessage) {
        // Tier 1: Try HAPI getUniversalID()
        try {
            String mflCode = omlMsg.getMSH().getSendingFacility().getUniversalID().getValue();
            if (mflCode != null && !mflCode.isEmpty() && mflCode.length() == 4) {
                LOG.info("MFL code extracted via HAPI UniversalID: {}", mflCode);
                return mflCode;
            }
            if (mflCode != null && !mflCode.isEmpty()) {
                LOG.info("HAPI UniversalID returned: '{}' (length={})", mflCode, mflCode.length());
            }
        } catch (Exception e) {
            LOG.info("HAPI UniversalID failed: {}", e.getMessage());
        }

        // Tier 2: Try alternative HAPI accessor (getHd2_UniversalID)
        try {
            String mflCode = omlMsg.getMSH().getSendingFacility().getHd2_UniversalID().getValue();
            if (mflCode != null && !mflCode.isEmpty() && mflCode.length() == 4) {
                LOG.info("MFL code extracted via HAPI Hd2_UniversalID: {}", mflCode);
                return mflCode;
            }
            if (mflCode != null && !mflCode.isEmpty()) {
                LOG.info("HAPI Hd2_UniversalID returned: '{}' (length={})", mflCode, mflCode.length());
            }
        } catch (Exception e) {
            LOG.info("HAPI Hd2_UniversalID failed: {}", e.getMessage());
        }

        // Tier 3: Manual string parsing of MSH-4
        // MSH-4 format: Name^MFL^Type (e.g., "Chelstone Zonal Health Centre^3886^URI")
        try {
            String[] lines = rawMessage.split("\r");
            for (String line : lines) {
                if (line.startsWith("MSH|")) {
                    String[] fields = line.split("\\|");
                    if (fields.length > 4 && !fields[4].isEmpty()) {
                        String mshField = fields[4];
                        LOG.info("MSH-4 raw field: '{}'", mshField);

                        // MSH-4 format: Name^MFL^Type
                        String[] components = mshField.split("\\^");
                        LOG.info("MSH-4 has {} components", components.length);

                        if (components.length >= 2 && !components[1].isEmpty()) {
                            String mflCode = components[1].trim();
                            LOG.info("Component[1] value: '{}'", mflCode);
                            if (mflCode.length() == 4 && mflCode.matches("\\d{4}")) {
                                LOG.info("MFL code extracted via manual MSH-4 parsing: {}", mflCode);
                                return mflCode;
                            }
                        }

                        // Tier 4: Try extracting any 4-digit sequence from the facility field
                        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\b(\\d{4})\\b");
                        java.util.regex.Matcher matcher = pattern.matcher(mshField);
                        if (matcher.find()) {
                            String mflCode = matcher.group(1);
                            LOG.info("MFL code extracted via regex from MSH-4: {}", mflCode);
                            return mflCode;
                        }
                    }
                    break;
                }
            }
        } catch (Exception e) {
            LOG.info("Manual MSH-4 parsing exception: {}", e.getMessage(), e);
        }

        LOG.warn("Failed to extract valid 4-digit MFL code using all methods");
        return null;
    }
}
