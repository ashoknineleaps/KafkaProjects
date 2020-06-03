package com.nineleaps.kafka.custom;

import java.util.Date;

public class Supplier {

	private int supplierId;
	
	private String supplierName;
	
	private Date supplierStartDate;

	public Supplier(int supplierId, String supplierName, Date supplierStartDate) {
		super();
		this.supplierId = supplierId;
		this.supplierName = supplierName;
		this.supplierStartDate = supplierStartDate;
	}

	public int getSupplierId() {
		return supplierId;
	}

	public String getSupplierName() {
		return supplierName;
	}

	public Date getSupplierStartDate() {
		return supplierStartDate;
	}
}
