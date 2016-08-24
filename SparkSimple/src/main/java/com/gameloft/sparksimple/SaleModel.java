package com.gameloft.sparksimple;

import java.io.Serializable;

/**
 * Schema model to use in spark
 * 
 * @author radu
 */
public class SaleModel implements Serializable {
	private final String clientId;
	private final Integer quantity;
	private final String productId;
	private final String storeId;
	
	public SaleModel(String clientId, Integer quantity, String productId, String storeId) {
		this.clientId = clientId;
		this.quantity = quantity;
		this.productId = productId;
		this.storeId = storeId;
	}

	public String getClientId() {
		return clientId;
	}

	public Integer getQuantity() {
		return quantity;
	}

	public String getProductId() {
		return productId;
	}

	public String getStoreId() {
		return storeId;
	}

}