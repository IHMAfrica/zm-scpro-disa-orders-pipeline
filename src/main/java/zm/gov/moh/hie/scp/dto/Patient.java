package zm.gov.moh.hie.scp.dto;

import zm.gov.moh.hie.scp.util.Gender;

import java.io.Serializable;

public class Patient implements Serializable {
    private static final long serialVersionUID = 1L;
    private String id;
    private String firstName;
    private String lastName;
    private Gender gender;
    private String dob;

    public Patient() {}

    public Patient(String id, String firstName, String lastName, Gender gender, String dob) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.gender = gender;
        this.dob = dob;
    }

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getFirstName() {
        return firstName;
    }
    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }
    public String getLastName() {
        return lastName;
    }
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    public Gender getGender() {
        return gender;
    }
    public void setGender(Gender gender) {
        this.gender = gender;
    }
    public String getDob() {
        return dob;
    }
    public void setDob(String dob) {
        this.dob = dob;
    }

    @Override
    public String toString() {
        return "Patient{" +
                "id='" + id + '\'' +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", gender=" + gender +
                ", dob=" + dob +
                '}';
    }
}
