package com.response;

public class Response {

	private String message;
	private String status;
	private String error_type;
	public Response() {
		super();

	}
	public Response(String message, String status, String error_type) {
		super();
		this.message = message;
		this.status = status;
		this.error_type = error_type;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}

	public String getError_type() {
		return error_type;
	}
	public void setError_type(String error_type) {
		this.error_type = error_type;
	}
}
