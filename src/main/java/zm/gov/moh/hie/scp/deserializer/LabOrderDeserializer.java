package zm.gov.moh.hie.scp.deserializer;

import ca.uhn.hl7v2.model.v25.message.OML_O21;
import ca.uhn.hl7v2.model.v25.segment.OBR;
import ca.uhn.hl7v2.model.v25.segment.ORC;
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

            // Sanitize message to normalize line endings
            String sanitizedMessage = sanitize(omlString);

            OML_O21 omlMsg = Hl7Parser.toOml021Message(sanitizedMessage);

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

            // Extract lab code from MSH-6
            String labCode = extractLabCode(omlMsg);

            // Extract order ID (ORC-2 Placer Order Number) using HAPI
            String orderId = extractOrderId(omlMsg);

            // Extract order date/time (OBR-7) using HAPI
            String orderDate = null;
            String orderTime = null;
            String[] dateTimeParts = extractDateTime(omlMsg);
            if (dateTimeParts != null) {
                orderDate = dateTimeParts[0];
                orderTime = dateTimeParts[1];
            }

            // Extract LOINC code from OBR-4 using HAPI
            String loinc = extractLoinc(omlMsg);
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
                    loinc,  // pass LOINC for SQL lookup
                    labCode,  // lab code extracted from MSH-6
                    omlString  // raw HL7 message for DLQ
            );

            LOG.info("Successfully deserialized LabOrder: orderId={}, messageRefId={}, mflCode={}, labCode={}, sendingApp={}, orderDate={}, orderTime={}",
                    orderId, messageRefId, mflCode, labCode, sendingApplication, orderDate, orderTime);
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
     * Extract Order ID from ORC-2 (Placer Order Number) using HAPI objects.
     * ORC-2: Placer Order Number (Entity Identifier)
     */
    private String extractOrderId(OML_O21 omlMsg) {
        try {
            String orderId = omlMsg.getORDER().getORC().getPlacerOrderNumber().getEntityIdentifier().getValue();
            if (orderId != null && !orderId.isEmpty()) {
                LOG.debug("Extracted order ID from ORC-2: {}", orderId);
                return orderId;
            }
        } catch (Exception e) {
            LOG.debug("Could not extract order ID from ORC-2: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Extract date/time from OBR-7 (Observation Date/Time) using HAPI objects.
     * OBR-7: Observation Date/Time (Timestamp)
     */
    private String[] extractDateTime(OML_O21 omlMsg) {
        try {
            String dateTimeStr = omlMsg.getORDER().getOBSERVATION_REQUEST().getOBR().getObservationDateTime().getTime().getValue();
            if (dateTimeStr != null && !dateTimeStr.isEmpty() && dateTimeStr.length() >= 12) {
                try {
                    LocalDateTime dt = LocalDateTime.parse(dateTimeStr.substring(0, 12) + "00",
                            zm.gov.moh.hie.scp.util.DateTimeUtil.DATETIME_DISA_FORMATTER);
                    String[] result = new String[]{
                            dt.format(zm.gov.moh.hie.scp.util.DateTimeUtil.DATE_FORMATTER),
                            dt.format(zm.gov.moh.hie.scp.util.DateTimeUtil.TIME_FORMATTER)
                    };
                    LOG.debug("Extracted date/time from OBR-7: {} {}", result[0], result[1]);
                    return result;
                } catch (Exception e) {
                    LOG.debug("Could not parse date/time '{}': {}", dateTimeStr, e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.debug("Could not extract date/time from OBR-7: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Extract LOINC code from OBR-4 (Universal Service ID) using HAPI objects.
     * OBR-4: Universal Service ID (Coded Element)
     * Format: <identifier>^<text>^<nameOfCodingSystem>^<alternateIdentifier>^<alternateText>^<nameOfAlternateCodingSystem>
     */
    private String extractLoinc(OML_O21 omlMsg) {
        try {
            // Try to get the main identifier (component 0)
            String loinc = omlMsg.getORDER().getOBSERVATION_REQUEST().getOBR().getUniversalServiceIdentifier().getIdentifier().getValue();
            if (loinc != null && !loinc.isEmpty()) {
                LOG.debug("Extracted LOINC code from OBR-4: {}", loinc);
                return loinc;
            }
            // Try alternate identifier (component 3)
            String altLoinc = omlMsg.getORDER().
                    getOBSERVATION_REQUEST().getOBR().getUniversalServiceIdentifier().getAlternateIdentifier().getValue();
            if (altLoinc != null && !altLoinc.isEmpty()) {
                LOG.debug("Extracted LOINC code (alternate) from OBR-4: {}", altLoinc);
                return altLoinc;
            }
        } catch (Exception e) {
            LOG.debug("Could not extract LOINC from OBR-4: {}", e.getMessage());
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

    String extractMflCodeRobust(OML_O21 omlMsg) {
        try {
            return omlMsg.getMSH().getSendingFacility().getUniversalID().getValue();
        } catch (Exception e) {
            LOG.debug("Could not extract MFL code from MSH-4: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Extract lab code from MSH-6 (Receiving Facility namespace ID) using HAPI.
     */
    private String extractLabCode(OML_O21 omlMsg) {
        try {
            String labCode = omlMsg.getMSH().getReceivingFacility().getNamespaceID().getValue();
            if (labCode != null && !labCode.isEmpty()) {
                LOG.debug("Extracted lab code from MSH-6: {}", labCode);
                return labCode;
            }
        } catch (Exception e) {
            LOG.debug("Could not extract lab code from MSH-6: {}", e.getMessage());
        }
        return null;
    }
}
