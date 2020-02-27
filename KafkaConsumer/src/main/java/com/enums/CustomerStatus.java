package com.enums;

public enum CustomerStatus {
	Open("O"), Close("C"), Suspended("S"), Restored("R");
	private String status;

	private String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	private CustomerStatus(String status) {
		this.status = status;
	}
}
