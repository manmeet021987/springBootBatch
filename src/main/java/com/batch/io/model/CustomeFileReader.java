package com.batch.io.model;

public class CustomeFileReader {
	
	private String fileline;
	
	public CustomeFileReader(){}

	public CustomeFileReader(String encryptedline) {
		fileline=encryptedline;
	}

	public String getFileline() {
		return fileline;
	}

	public void setFileline(String fileline) {
		this.fileline = fileline;
	}

}
