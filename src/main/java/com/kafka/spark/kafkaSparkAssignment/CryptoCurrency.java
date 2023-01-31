package com.kafka.spark.kafkaSparkAssignment;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CryptoCurrency implements Serializable {

	private static final long serialVersionUID = 1L;

	private String symbol;
	private String timestamp;

	private PriceData priceData;

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public PriceData getPriceData() {
		return priceData;
	}

	public void setPriceData(PriceData priceData) {
		this.priceData = priceData;
	}

	@Override
	public String toString() {
		return "crypto [symbol=" + symbol + ", timestamp=" + timestamp + ", priceData=" + priceData + "]";
	}

}
