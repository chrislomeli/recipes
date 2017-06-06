package com.temp.worksheet;

import java.awt.Color;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFCreationHelper;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.xssf.usermodel.extensions.XSSFCellBorder.BorderSide;


public class SQLWorkbook {

    XSSFWorkbook  workbook = new XSSFWorkbook(); 
    Map<String, Worksheet> worksheets = new HashMap<>();

	public class Worksheet {
		XSSFSheet workSheet;
		int rownum;
		int column_count;
		
		public Worksheet(XSSFSheet xws) {
			this.workSheet = xws;
			this.rownum = 0;
			this.column_count = 0;
		}
		public XSSFSheet getXssfSheet() {
			return workSheet;
		}
		public void setXssfSheet(XSSFSheet xssfSheet) {
			this.workSheet = xssfSheet;
		}
		public Row nextRow() {
			return workSheet.createRow(this.rownum++);
		}
	}
	
    
    XSSFCellStyle matrix_style = workbook.createCellStyle();
    XSSFCellStyle bold_break_style = workbook.createCellStyle();
    XSSFCellStyle break_style = workbook.createCellStyle();
    XSSFCellStyle label_style = workbook.createCellStyle();
    XSSFCellStyle heading_style = workbook.createCellStyle();
	
	public SQLWorkbook () {
    	this.setStyles();
	}
	

	public void print(String fileName) throws IOException {
		
		for (Worksheet ws : worksheets.values()) {
			
			for (int i=1; i<=ws.column_count; i++) {
				ws.workSheet.autoSizeColumn(i);
			}
		}
		
        FileOutputStream out = new FileOutputStream(new File(fileName));
        workbook.write(out);
        out.close();
	}
	
	public void addWorksheet(String id) throws IOException {
		
        XSSFSheet sheet = workbook.createSheet(id);
        sheet.setDefaultColumnWidth(15);
        
        this.worksheets.put(id, new Worksheet(sheet));
	}
	
	
	/**
	 */
	public void setHeading(String id,  List<String> labels) {

		Worksheet wsheet = worksheets.get(id);

		
		XSSFSheet sheet = wsheet.getXssfSheet();
		Row row = wsheet.nextRow();

		int i=0;
		for (String label : labels ) {
			
			if (label==null) {
				label="[blank]";
			}

			XSSFCellStyle style = workbook.createCellStyle() ;
			style.cloneStyleFrom(heading_style);
			
			Cell cell = row.createCell(i++);
	        cell.setCellStyle(style);
			
			cell.setCellValue(label);
            cell.setCellStyle(style);
		}
	}
	
	
	/**
	 * setRow  - alternative helper function to set an class containing a raw array of cells 
	 * from a single row of a memory matrix into a sheet
	 */
	public void setRow(String id, List<Object> values) {
		
        Worksheet wsheet = worksheets.get(id);
        XSSFSheet sheet = wsheet.getXssfSheet();
		Row row = wsheet.nextRow();
		wsheet.column_count = Math.max( values.size(), wsheet.column_count );
		
		XSSFCellStyle reqStyle = matrix_style;
		XSSFCreationHelper helper = workbook.getCreationHelper();

		int i = 0;
		for (Object obj : values ) {

			XSSFCellStyle style = workbook.createCellStyle() ;
			style.cloneStyleFrom(reqStyle);
			Cell cell = row.createCell(i++);
	        cell.setCellStyle(style);
			
			if (obj==null) {
				obj = new String("<null>");
			}
			
			if (obj instanceof Integer) {
				cell.setCellValue((Integer) obj);
				
			} else if (obj instanceof Double) {
				cell.setCellValue((Double) obj);
				style.setDataFormat(helper.createDataFormat().getFormat("#,##0.000_);[Red](#,##0.000)"));
				
			} else if (obj instanceof BigDecimal ) {
				BigDecimal 	bd = (BigDecimal)obj;
				Double dbl = bd.doubleValue();
				cell.setCellValue(dbl);
				
				style.setDataFormat(helper.createDataFormat().getFormat("#,##0.000_);[Red](#,##0.00)"));
				
				
				
			} else if (obj instanceof Date) {
				cell.setCellValue((Date)obj);
				style.setDataFormat(helper.createDataFormat().getFormat("[$-409]d-mmm-yyyy;@"));
				
			} else if (obj instanceof String) {
				cell.setCellValue((String)obj);
				
			} else if (obj != null) {
				cell.setCellValue(obj.toString());
			}
            cell.setCellStyle(style);
		}
		
	}
	
	
	public void setStyles() {
   	
		float[] blood = new float[3]; Color.RGBtoHSB(15,37,63,blood);
		float[] tan = new float[3]; Color.RGBtoHSB(238,236,225,tan);
		float[] hsb = new float[3]; Color.RGBtoHSB(197,217,241,hsb);
		   
		XSSFColor color_dark_blue		=new XSSFColor(Color.getHSBColor(blood[0], blood[1], blood[2]));
		XSSFColor color_tan				=new XSSFColor(Color.getHSBColor(tan[0], tan[1], tan[2]));
		XSSFColor color_light_blue		=new XSSFColor(Color.getHSBColor(1.58f,.364f,.871f));
		XSSFColor color_blue			=new XSSFColor(Color.getHSBColor(hsb[0], hsb[1], hsb[2]));
		
		
		XSSFFont rfont = workbook.createFont();
		rfont.setColor(color_dark_blue);
		rfont.setFontHeight((double)10);
		rfont.setFontName("Calibri");
		
		XSSFFont hfont = workbook.createFont();
		hfont.setColor(color_dark_blue);
		hfont.setFontHeight((double)10);
		hfont.setFontName("Calibri");
		hfont.setBold(true);
		 	
		break_style.setBorderTop(CellStyle.BORDER_THIN);
		break_style.setBorderColor(BorderSide.TOP, color_light_blue);
		break_style.setBorderColor(BorderSide.RIGHT, color_light_blue);
		break_style.setBorderColor(BorderSide.LEFT, color_light_blue);
		break_style.setBorderColor(BorderSide.RIGHT, color_light_blue);
		break_style.setBorderColor(BorderSide.LEFT, color_light_blue);
		break_style.setFont(rfont);
		 	
		matrix_style.setFillBackgroundColor(IndexedColors.AUTOMATIC.getIndex());
		matrix_style.setFillForegroundColor(IndexedColors.AUTOMATIC.getIndex());
		matrix_style.setFont(rfont);
		
		bold_break_style.cloneStyleFrom(matrix_style);
		bold_break_style.setBorderTop(CellStyle.BORDER_THIN);
		bold_break_style.setBorderColor(BorderSide.TOP, color_dark_blue);
			
		heading_style.setFillForegroundColor(color_blue);
		heading_style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		heading_style.setBorderRight(CellStyle.BORDER_THIN);
		heading_style.setRightBorderColor(color_blue);
		heading_style.setBorderLeft(CellStyle.BORDER_THIN);
		heading_style.setLeftBorderColor(color_blue);
		heading_style.setBorderTop(CellStyle.BORDER_THIN);
		heading_style.setTopBorderColor(color_blue);
		heading_style.setBorderBottom(CellStyle.BORDER_THIN);
		heading_style.setBottomBorderColor(color_blue);
		heading_style.setFont(hfont);
		 	
		 	
		label_style.setFillForegroundColor(color_tan);
		label_style.setFillPattern(CellStyle.SOLID_FOREGROUND);
		label_style.setBorderRight(CellStyle.BORDER_THIN);
		label_style.setRightBorderColor(color_tan);
		label_style.setBorderLeft(CellStyle.BORDER_THIN);
		label_style.setLeftBorderColor(color_tan);
		label_style.setBorderTop(CellStyle.BORDER_THIN);
		label_style.setTopBorderColor(color_tan);
		label_style.setBorderBottom(CellStyle.BORDER_THIN);
		label_style.setBottomBorderColor(color_tan);
		label_style.setFont(hfont);
		
		
	}
	
}
	
	

