package com.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name="ERROR_LOG")
@Entity
public class ErrorLog {
	
	@Id
	@GeneratedValue
	private int id;
	private String error_type;
	@Column(length=5000)
	private String error_description;
	@Column(length=5000)
	private String payload;
    public ErrorLog() {
		super();
		
	}
	public ErrorLog(int id, String error_type, String error_description, String payload) {
		super();
		this.id = id;
		this.error_type = error_type;
		this.error_description = error_description;
		this.payload = payload;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getError_type() {
		return error_type;
	}
	public void setError_type(String error_type) {
		this.error_type = error_type;
	}
	public String getError_description() {
		return error_description;
	}
	public void setError_description(String error_description) {
		this.error_description = error_description;
	}
	public String getPayload() {
		return payload;
	}
	public void setPayload(String payload) {
		this.payload = payload;
	}

}
