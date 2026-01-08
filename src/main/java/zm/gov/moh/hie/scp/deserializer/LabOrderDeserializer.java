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
            String messageRefId = extractMessageControlId(omlMsg);

            if (messageRefId == null || messageRefId.isEmpty()) {
                LOG.debug("Message control ID is empty, skipping message");
                return null;
            }

            // Extract sending application (MSH-3)
            String sendingApplication = extractSendingApplication(omlMsg);

            // Extract MFL code from MSH-4
            String mflCode = extractMflCodeRobust(omlMsg);

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

    /**
     * Extract Message Control ID from MSH-10 using HAPI.
     */
    private String extractMessageControlId(OML_O21 omlMsg) {
        try {
            String messageControlId = omlMsg.getMSH().getMessageControlID().getValue();
            if (messageControlId != null && !messageControlId.isEmpty()) {
                LOG.debug("Extracted message control ID: {}", messageControlId);
                return messageControlId;
            }
        } catch (Exception e) {
            LOG.debug("Could not extract message control ID from MSH-10: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Extract Sending Application from MSH-3 using HAPI.
     */
    private String extractSendingApplication(OML_O21 omlMsg) {
        try {
            String sendingApp = omlMsg.getMSH().getSendingApplication().getNamespaceID().getValue();
            if (sendingApp != null && !sendingApp.isEmpty()) {
                LOG.debug("Extracted sending application: {}", sendingApp);
                return sendingApp;
            }
        } catch (Exception e) {
            LOG.debug("Could not extract sending application from MSH-3: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Extract Order ID from ORC-2 (Placer Order Number) by string parsing.
     */
    private String extractOrderIdFromMessage(String message) {
        String[] lines = message.split("\r");
        for (String line : lines) {
            if (line.startsWith("ORC|")) {
                String[] fields = line.split("\\|");
                if (fields.length > 2 && !fields[2].isEmpty()) {
                    String orderId = fields[2].split("\\^")[0];
                    LOG.debug("Extracted order ID: {}", orderId);
                    return orderId;
                }
            }
        }
        return null;
    }

    /**
     * Extract date/time from OBR-7 (Observation Date/Time) by string parsing.
     */
    private String[] extractDateTimeFromMessage(String message) {
        String[] lines = message.split("\r");
        for (String line : lines) {
            if (line.startsWith("OBR|")) {
                String[] fields = line.split("\\|");
                if (fields.length > 7 && !fields[7].isEmpty()) {
                    try {
                        String dateTimeStr = fields[7];
                        if (dateTimeStr.length() >= 12) {
                            LocalDateTime dt = LocalDateTime.parse(dateTimeStr.substring(0, 12) + "00",
                                    zm.gov.moh.hie.scp.util.DateTimeUtil.DATETIME_DISA_FORMATTER);
                            String[] result = new String[]{
                                    dt.format(zm.gov.moh.hie.scp.util.DateTimeUtil.DATE_FORMATTER),
                                    dt.format(zm.gov.moh.hie.scp.util.DateTimeUtil.TIME_FORMATTER)
                            };
                            LOG.debug("Extracted date/time: {} {}", result[0], result[1]);
                            return result;
                        }
                    } catch (Exception e) {
                        LOG.debug("Could not parse date/time: {}", fields[7]);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Extract LOINC code from OBR-4 (Universal Service ID) by string parsing.
     */
    private String extractLoincFromMessage(String message) {
        String[] lines = message.split("\r");
        for (String line : lines) {
            if (line.startsWith("OBR|")) {
                String[] fields = line.split("\\|");
                if (fields.length > 4 && !fields[4].isEmpty()) {
                    try {
                        String obrField = fields[4];
                        String[] components = obrField.split("\\^");
                        if (components.length > 0 && !components[0].isEmpty()) {
                            LOG.debug("Extracted LOINC code: {}", components[0]);
                            return components[0];
                        }
                        if (components.length > 3 && !components[3].isEmpty()) {
                            LOG.debug("Extracted LOINC code (alternate): {}", components[3]);
                            return components[3];
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
     * Extract MFL code from MSH-4 (SendingFacility) using HAPI.
     * MSH-4 format: Name^UniversalID^UniversalIDType
     *
     * Component structure:
     * - Component[0] (NamespaceID): Facility name
     * - Component[1] (UniversalID): 4-digit MFL code
     * - Component[2] (UniversalIDType): Type identifier (e.g., URI)
     */
    String extractMflCodeRobust(OML_O21 omlMsg) {
        try {
            // Get the SendingFacility field (MSH-4)
            ca.uhn.hl7v2.model.v25.datatype.HD sendingFacility = omlMsg.getMSH().getSendingFacility();

            if (sendingFacility == null) {
                LOG.debug("SendingFacility (MSH-4) is null");
                return null;
            }

            // Extract UniversalID (component[1]) which contains the 4-digit MFL code
            try {
                String mflCode = sendingFacility.getUniversalID().getValue();
                if (mflCode != null && !mflCode.isEmpty()) {
                    LOG.debug("Extracted MFL code from UniversalID: {}", mflCode);
                    return mflCode;
                }
            } catch (Exception e) {
                LOG.debug("Could not extract UniversalID from SendingFacility: {}", e.getMessage());
            }

        } catch (Exception e) {
            LOG.debug("Error extracting MFL code from SendingFacility: {}", e.getMessage());
        }

        LOG.debug("Failed to extract MFL code from MSH-4");
        return null;
    }
}
