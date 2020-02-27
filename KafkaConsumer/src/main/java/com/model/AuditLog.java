package com.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "AUDIT_LOG")
@Entity
public class AuditLog {

	@Id
	@GeneratedValue
	private int id;
	private String customerNumber;
	@Column(length=5000)
	private String payload;
	public int getId() {
		return id;
	}
	public AuditLog() {
		super();
		// TODO Auto-generated constructor stub
	}
	public AuditLog(int id, String customerNumber, String payload) {
		super();
		this.id = id;
		this.customerNumber = customerNumber;
		this.payload = payload;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getCustomerNumber() {
		return customerNumber;
	}
	public void setCustomerNumber(String customerNumber) {
		this.customerNumber = customerNumber;
	}
	public String getPayload() {
		return payload;
	}
	public void setPayload(String payload) {
		this.payload = payload;
	}
}
