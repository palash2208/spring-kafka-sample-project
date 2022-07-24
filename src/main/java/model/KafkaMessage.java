package model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class KafkaMessage implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String message;

	public void setMessage(String message) {
		this.message = message;
	}

	public String getMessage() {
		return message;
	}
	
	
	
}
