package zm.gov.moh.hie.scp.util;

import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v25.message.OML_O21;
import ca.uhn.hl7v2.model.v25.message.ORU_R01;
import ca.uhn.hl7v2.model.v25.segment.MSH;
import ca.uhn.hl7v2.model.v25.segment.PID;
import ca.uhn.hl7v2.parser.PipeParser;
import zm.gov.moh.hie.scp.dto.Header;
import zm.gov.moh.hie.scp.dto.Patient;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Hl7Parser {
    public static ORU_R01 toOru021Message(String hl7String) {
        try {
            Message msg = (new PipeParser()).parse(hl7String);
            return (ORU_R01) msg;
        } catch (Exception ex) {
            System.out.println("Exception when parsing HL7 ORU Message: " + ex.getMessage());
        }
        return null;
    }

    public static OML_O21 toOml021Message(String hl7String) {
        try {
            Message msg = (new PipeParser()).parse(hl7String);
            return (OML_O21) msg;
        } catch (Exception ex) {
            System.out.println("Exception when parsing HL7 OML Message: " + ex.getMessage());
        }
        return null;
    }

    public static Patient toPatient(PID patient) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        String id = patient.getPatientIdentifierList(0).getIDNumber().getValue();
        String firstName = patient.getPatientName(0).getGivenName().getValue();
        String lastName = patient.getPatientName(0).getFamilyName().getSurname().getValue();
        Gender gender = switch (patient.getAdministrativeSex().getValue()) {
            case "M" -> Gender.MALE;
            case "F" -> Gender.FEMALE;
            default -> Gender.OTHER;
        };
        LocalDate dob = LocalDate.parse(patient.getDateTimeOfBirth().getTime().getValue(), dateFormatter);

        return new Patient(id, firstName, lastName, gender, dob.format(DateTimeUtil.DATE_FORMATTER));
    }

    public static Header toHeader(MSH header) {
        MessageType type = switch (header.getMessageType().getMessageStructure().getValue()) {
            case "ORU_R01" -> MessageType.ORU_R01;
            case "OML_021" -> MessageType.OML_021;
            default -> MessageType.ACK;
        };
        String version = header.getVersionID().getVersionID().getValue();
        LocalDateTime timestamp = LocalDateTime.parse(
                header.getDateTimeOfMessage().getTime().getValue(),
                DateTimeUtil.DATETIME_DISA_FORMATTER);
        String messageId = header.getMessageControlID().getValue();
        String ackType = header.getApplicationAcknowledgmentType().getValue();
        String senderId = header.getSendingFacility().getUniversalID().getValue();
        String receiverId = header.getReceivingFacility().getNamespaceID().getValue();

        return new Header(type, version, timestamp.format(DateTimeUtil.TIMESTAMP_FORMATTER), senderId, receiverId, messageId, ackType);
    }
}
