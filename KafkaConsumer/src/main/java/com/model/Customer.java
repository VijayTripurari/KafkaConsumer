package com.model;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.validation.constraints.Email;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Entity
public class Customer {

	@Id
	@GeneratedValue
	private int id;
	
	@Size(max=10, message="Customer Number can not be more than 10")
	@NotBlank
	private String customerNumber;

	@Size(max=50, message="First Name can not be more than 50 characters")
	@Size(min=10, message="First Name can not be less than 10 characters")
	@NotBlank
	private String firstName;

	@Size(max=50, message="Last Name can not be more than 50 characters")
	@Size(min=10, message="Last Name can not be less than 10 characters")
	@NotBlank
	private String lastName;

	private String birthDate;

	@NotBlank
	private String country;
	
	@Size(max=2, message="Country code can not be more than 2")
	@NotBlank
	private String countryCode;

	@Size(min=10, message="Mobile Number can not be more than 10 characters")
	@NotBlank
	private String mobileNumber;

	@Size(max=50, message="email can not be more than 50 characters")
	@Email
	private String email;

	private String customerStatus;

	@NotNull
	@OneToOne(cascade=CascadeType.PERSIST)
	private Address address;

	public Customer() {
		super();
	}

	public Customer(String customerNumber, String firstName, String lastName, String birthDate, String country,
			String countryCode, String mobileNumber, String email, String customerStatus, Address address) {
		super();
		this.customerNumber = customerNumber;
		this.firstName = firstName;
		this.lastName = lastName;
		this.birthDate = birthDate;
		this.country = country;
		this.countryCode = countryCode;
		this.mobileNumber = mobileNumber;
		this.email = email;
		this.customerStatus = customerStatus;
		this.address = address;
	}

	public String getCustomerNumber() {
		return customerNumber;
	}

	public void setCustomerNumber(String customerNumber) {
		this.customerNumber = customerNumber;
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

	public String getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(String birthDate) {
		this.birthDate = birthDate;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getMobileNumber() {
		return mobileNumber;
	}

	public void setMobileNumber(String mobileNumber) {
		this.mobileNumber = mobileNumber;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getCustomerStatus() {
		return customerStatus;
	}

	public void setCustomerStatus(String customerStatus) {
		this.customerStatus = customerStatus;
	}

	public Address getAddress() {
		return address;
	}

	public void setAddress(Address address) {
		this.address = address;
	}

	@Override
	public String toString() {
		return "Customer [customerNumber=" + customerNumber + ", firstName=" + firstName + ", lastName=" + lastName
				+ ", birthDate=" + birthDate + ", country=" + country + ", countryCode=" + countryCode
				+ ", mobileNumber=" + mobileNumber + ", email=" + email + ", customerStatus=" + customerStatus
				+ ", address=" + address + "]";
	}

}
