package com.phani.demo.notifications;

import com.google.gson.annotations.SerializedName;

/**
 * Created by mudassir on 11/01/19.
 */
public class SchemaColumnDetail {

	private String type;

	private int order;

	private int size;

	private boolean active;

	@SerializedName( "date_format" )
	private String dateFormat;

	public SchemaColumnDetail()
	{
	}

	public SchemaColumnDetail(String type, int order, int size, boolean active, String dateFormat )
	{
		this.type = type;
		this.order = order;
		this.size = size;
		this.active = active;
		this.dateFormat = dateFormat;
	}

	public String getDataType()
	{
		return type;
	}

	public void setDataType( String dataType )
	{
		this.type = dataType;
	}

	public int getOrder()
	{
		return order;
	}

	public void setOrder( int order )
	{
		this.order = order;
	}

	public int getSize()
	{
		return size;
	}

	public void setSize( int size )
	{
		this.size = size;
	}

	public boolean isActive()
	{
		return active;
	}

	public void setActive( boolean active )
	{
		this.active = active;
	}

	public String getDateFormat()
	{
		return dateFormat;
	}

	public void setDateFormat( String dateFormat )
	{
		this.dateFormat = dateFormat;
	}
}
