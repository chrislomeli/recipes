package com.temp.worksheet;

/*
 * Column class
 */
public class Column {


	/* 
	 * 	new Column(<table id>,<column Name>, <label>, <format>, <macro>, <visible t/f>) );

	 * sample
	 * 	fieldChooser.add( new Column("e","connect_date", "ConnectDate", "decimal","to_char(?, 'hh24:mi:ss')", true) );
	 */
	String tableId;
	String columnName;
	String columnLabel;
	String format;
	String macro;
	boolean visible;
			
	
	public Column (String table, String name, String label, String format, String macro, boolean show) {
		this.tableId = table;
		this.columnName = name;
		this.columnLabel  = label;
		this.format = format;
		this.macro=macro;
		this.visible = show;
	}


	public String getTableId() {
		return tableId;
	}


	public void setTableId(String tableId) {
		this.tableId = tableId;
	}


	public String getColumnName() {
		return columnName;
	}


	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}


	public String getColumnLabel() {
		return columnLabel;
	}


	public void setColumnLabel(String columnLabel) {
		this.columnLabel = columnLabel;
	}


	public String getFormat() {
		return format;
	}


	public void setFormat(String format) {
		this.format = format;
	}


	public String getMacro() {
		return macro;
	}


	public void setMacro(String macro) {
		this.macro = macro;
	}


	public boolean isVisible() {
		return visible;
	}


	public void setVisible(boolean visible) {
		this.visible = visible;
	}
	
	
	
	
}
