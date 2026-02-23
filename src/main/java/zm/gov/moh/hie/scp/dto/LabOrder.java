package zm.gov.moh.hie.scp.dto;

import java.io.Serializable;

public class LabOrder implements Serializable {
    private static final long serialVersionUID = 1L;
    private Header header;
    private String mflCode;
    private String orderId;
    private Short testId;
    private String orderDate;
    private String orderTime;
    private String messageRefId;
    private String sendingApplication;
    private String loinc;
    private String labCode;
    private String rawMessage;

    public LabOrder() {}

    public LabOrder(Header header, String mflCode, String orderId, Short testId, String orderDate, String orderTime, String messageRefId, String sendingApplication, String loinc, String labCode, String rawMessage) {
        this.header = header;
        this.mflCode = mflCode;
        this.orderId = orderId;
        this.testId = testId;
        this.orderDate = orderDate;
        this.orderTime = orderTime;
        this.messageRefId = messageRefId;
        this.sendingApplication = sendingApplication;
        this.loinc = loinc;
        this.labCode = labCode;
        this.rawMessage = rawMessage;
    }

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public String getMflCode() {
        return mflCode;
    }

    public void setMflCode(String mflCode) {
        this.mflCode = mflCode;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Short getTestId() {
        return testId;
    }

    public void setTestId(Short testId) {
        this.testId = testId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public String getMessageRefId() {
        return messageRefId;
    }

    public void setMessageRefId(String messageRefId) {
        this.messageRefId = messageRefId;
    }

    public String getSendingApplication() {
        return sendingApplication;
    }

    public void setSendingApplication(String sendingApplication) {
        this.sendingApplication = sendingApplication;
    }

    public String getLoinc() {
        return loinc;
    }

    public void setLoinc(String loinc) {
        this.loinc = loinc;
    }

    public String getLabCode() {
        return labCode;
    }

    public void setLabCode(String labCode) {
        this.labCode = labCode;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(String rawMessage) {
        this.rawMessage = rawMessage;
    }

    @Override
    public String toString() {
        return "LabOrder{" +
                "header=" + header +
                ", mflCode='" + mflCode + '\'' +
                ", orderId='" + orderId + '\'' +
                ", testId=" + testId +
                ", orderDate='" + orderDate + '\'' +
                ", orderTime='" + orderTime + '\'' +
                ", messageRefId='" + messageRefId + '\'' +
                ", sendingApplication='" + sendingApplication + '\'' +
                ", labCode='" + labCode + '\'' +
                '}';
    }
}
