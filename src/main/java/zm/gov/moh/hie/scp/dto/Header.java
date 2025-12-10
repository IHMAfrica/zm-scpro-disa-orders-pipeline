package zm.gov.moh.hie.scp.dto;

import zm.gov.moh.hie.scp.util.MessageType;

import java.io.Serializable;

public class Header implements Serializable {
    private static final long serialVersionUID = 1L;
    private MessageType messageType;
    private String version;
    private String timestamp;
    private String senderId;
    private String receiverId;
    private String messageId;
    private String ackType;

    public Header() {
    }

    public Header(MessageType messageType, String version, String timestamp, String senderId, String receiverId, String messageId, String ackType) {
        this.messageType = messageType;
        this.version = version;
        this.timestamp = timestamp;
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.messageId = messageId;
        this.ackType = ackType;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public String getReceiverId() {
        return receiverId;
    }

    public void setReceiverId(String receiverId) {
        this.receiverId = receiverId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getAckType() {
        return ackType;
    }

    public void setAckType(String ackType) {
        this.ackType = ackType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Header{");
        sb.append("messageType=").append(messageType)
                .append(", messageId='").append(messageId).append('\'')
                .append(", version='").append(version).append('\'')
                .append(", timestamp=").append(timestamp)
                .append(", senderId='").append(senderId).append('\'')
                .append(", receiverId='").append(receiverId).append('\'');

        if (ackType != null) {
            sb.append(", ackType='").append(ackType).append('\'');
        }
        sb.append('}');
        return sb.toString();
    }
}
