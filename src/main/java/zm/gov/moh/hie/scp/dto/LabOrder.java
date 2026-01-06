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

    public LabOrder() {}

    public LabOrder(Header header, String mflCode, String orderId, Short testId, String orderDate, String orderTime, String messageRefId, String sendingApplication, String loinc) {
        this.header = header;
        this.mflCode = mflCode;
        this.orderId = orderId;
        this.testId = testId;
        this.orderDate = orderDate;
        this.orderTime = orderTime;
        this.messageRefId = messageRefId;
        this.sendingApplication = sendingApplication;
        this.loinc = loinc;
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
                '}';
    }
}
