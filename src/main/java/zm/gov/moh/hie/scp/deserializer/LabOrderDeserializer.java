package zm.gov.moh.hie.scp.deserializer;

import ca.uhn.hl7v2.model.v25.message.OML_O21;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.dto.Header;
import zm.gov.moh.hie.scp.dto.LabOrder;
import zm.gov.moh.hie.scp.util.DateTimeUtil;
import zm.gov.moh.hie.scp.util.Hl7Parser;

import java.time.LocalDateTime;

public class LabOrderDeserializer implements DeserializationSchema<LabOrder> {
    private static final Logger LOG = LoggerFactory.getLogger(LabOrderDeserializer.class);

    @Override
    public LabOrder deserialize(byte[] bytes) {
        try {
            String omlString = new String(bytes);
            OML_O21 omlMsg = Hl7Parser.toOml021Message(sanitize(omlString));

            if (omlMsg == null) {
                LOG.warn("Failed to parse OML message");
                return null;
            }

            Header header = Hl7Parser.toHeader(omlMsg.getMSH());

            if (header == null) {
                LOG.warn("Failed to extract header from OML message");
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

            LabOrder labOrder = null;

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
                labOrder = new LabOrder(
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
            }

            return labOrder;
        } catch (Exception e) {
            LOG.error("Error deserializing LabOrder: {}", e.getMessage(), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(LabOrder nextElement) {
        return false;
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

    @Override
    public TypeInformation<LabOrder> getProducedType() {
        return TypeInformation.of(LabOrder.class);
    }
}
