package com.temp;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Currency;
import java.util.Locale;

import javax.imageio.ImageIO;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.itextpdf.text.BadElementException;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Chunk;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.Font.FontFamily;
import com.itextpdf.text.Image;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.html.WebColors;
import com.itextpdf.text.pdf.ColumnText;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfPageEventHelper;
import com.itextpdf.text.pdf.PdfWriter;

 

public class PDFStatementFormatter {
	/*
	 * Blue theme
	 */
	// BLUE BaseColor header2Background = WebColors.getRGBColor("#2F4F4F"); // header
	// column 2
	BaseColor header2Background = WebColors.getRGBColor("#2041EA"); // header
																	// column 2

	// Light GREY BaseColor header1_background = WebColors.getRGBColor("#F5F5DC"); //
	// header column 1FFA07A
	BaseColor header1_background = WebColors.getRGBColor("#D3D3D3"); // header
																		// column
																		// 1FFA07A
	BaseColor header1_font = BaseColor.DARK_GRAY; //

	// BROWN GRAY BaseColor tbl_hdr_background = WebColors.getRGBColor("#5F9EA0"); // table
	// row heading background
	BaseColor tbl_hdr_background = WebColors.getRGBColor("#5A5050"); // table
																		// row
																		// heading
	BaseColor tbl_hdr_font_color = BaseColor.WHITE; // was WHITE
	BaseColor tbl_cell_background = BaseColor.WHITE; // table cell background
	BaseColor tbl_cell_font_color = BaseColor.DARK_GRAY; // table data cell font
	BaseColor tbl_borderColor = BaseColor.WHITE;

	int col_pad_horiz = 5;
	int col_pad_vertical = 4;

	FontFamily font_family = FontFamily.HELVETICA;

	Font h1_normal_font = new Font(font_family, 14);
	Font h1_inverse_font = new Font(font_family, 14);
	Font h2_normal_font = new Font(font_family, 12);
	Font normal_strong_font = new Font(font_family, 9f);
	Font h2_inverse_font = new Font(font_family, 10);
	Font normal_font = new Font(font_family, 8.5f);
	Font item_font = new Font(font_family, 7f);
	Font normal_inverse_font = new Font(font_family, 9f);
	Font tbl_hdr_font = new Font(font_family, 8.5f);
	Font small_font = new Font(font_family, 6);
	Font legal_font = new Font(font_family, 8.5f);

	
	String summaryStr =  "Summary";
	String rateLineStr = "Line Items";
	String surchargeStr = "Fees";
	
   
	DateTimeFormatter monthyearFmt = DateTimeFormat.forPattern("MMMM yyyy");
	DateTimeFormatter dateFormat = DateTimeFormat.forPattern("MM-dd-yyyy");
	
	
	
	/**
     * 
     * MAIN PDF creation method
     * 
     * 
     */
    public void createPdf(Statement statement, String fullPath, String logofilename) throws IOException, DocumentException {

        Document document = new Document(PageSize.LETTER);
        document.setMargins(45, 45, 45, 45);
        
		float logoScale = 75;
		
         
        PdfWriter writer = PdfWriter.getInstance(document, new FileOutputStream(fullPath));
        HeaderFooter event = new HeaderFooter();
        writer.setBoxSize("art", new Rectangle(36, 54, 559, 788));
        writer.setPageEvent(event);
 
        
        document.open();
        
        /*
         *  logo and address block
         */
        document.add(getLogo(logofilename, logoScale));
         document.add(Chunk.NEWLINE);
         document.add(Chunk.NEWLINE);
        

        /*
         *  Header Block
         */
        PdfPTable htable = getHeaderTable(statement);
        document.add(htable);
        
        /**
         * Spacing and font change
         */
        Paragraph p3 = new Paragraph("Statement "+summaryStr);
        p3.setFont(h2_normal_font);
        p3.setAlignment(Element.ALIGN_LEFT);
        p3.setSpacingBefore(20);
        p3.setSpacingAfter(10);
        document.add(p3);

        /**
         * Bill Summary Block 
         * 
         */
        PdfPTable table = getStatementSummaryTable(statement);
        if (table != null) {
        	document.add(table);
        }
        
 
        /**
         * Item detail Block
         * 
         */
         PdfPTable itable  = getStatementItemTable(statement);
        if (itable != null) {

            Paragraph p2 = new Paragraph("Statement "+rateLineStr);
            p2.setFont(h2_normal_font);
            p2.setAlignment(Element.ALIGN_LEFT);
            p2.setSpacingBefore(20);
            p2.setSpacingAfter(10);
            document.add(p2);

        	document.add(itable);
        }
        
        document.close();
    }	
	
	
 
	public PdfPTable getTotalTable (Statement statement) {
 		Locale loc = Locale.forLanguageTag(statement.getLanguage());
		NumberFormat currency = NumberFormat.getCurrencyInstance(loc);
		Currency cur = Currency.getInstance(statement.getCurrencyCode());
		currency.setCurrency(cur);
    
	    PdfPTable table  = new PdfPTable(4);
	    table.setWidthPercentage(30);
	    
	    // HEADER Cell Left
        PdfPCell cell = new PdfPCell();
        cell.setColspan(2);
        cell.setPaddingTop(0);
        cell.setPaddingBottom(0);
        cell.setPaddingRight(0);
        cell.setPaddingLeft(0);
        cell.setBackgroundColor(header1_background);
        cell.setBorderColor(header1_background);
        cell.setUseDescender(true);
        
        // Header cell left - title
        Paragraph p5 = new Paragraph("Total Amount");
        p5.setFont(normal_strong_font);
        p5.setAlignment(Element.ALIGN_LEFT);
        cell.addElement(p5);
        table.addCell(cell);

	    // HEADER Cell Left
        PdfPCell cell1 = new PdfPCell();
        cell1.setColspan(2);
        cell1.setPaddingTop(0);
        cell1.setPaddingBottom(0);
        cell1.setPaddingRight(0);
        cell1.setPaddingLeft(0);
        cell1.setBackgroundColor(header1_background);
        cell1.setBorderColor(header1_background);
        cell1.setUseDescender(true);
        
        // Header cell left - title
       
        String mebo = currency.format(statement.getFinalCharge());
        
        Paragraph p6 = new Paragraph(mebo);
        p6.setFont(normal_strong_font);
        p6.setAlignment(Element.ALIGN_RIGHT);
        cell1.addElement(p6);
        table.addCell(cell1);
        table.completeRow();

	    // HEADER Cell Left
        cell1 = new PdfPCell();
        cell1.setColspan(4);
        cell1.setPaddingTop(0);
        cell1.setPaddingBottom(0);
        cell1.setPaddingRight(0);
        cell1.setPaddingLeft(0);
        cell1.setBackgroundColor(header1_background);
        cell1.setBorderColor(header1_background);
        cell1.setUseDescender(true);
        
        // Header cell left - title
        p6 = new Paragraph(statement.getCurrencyCode());
        p6.setFont(h1_normal_font);
        p6.setAlignment(Element.ALIGN_RIGHT);
        cell1.addElement(p6);
        table.addCell(cell1);
        table.completeRow();
	    return table;
	}
    
	public PdfPTable getHeaderTable (Statement statement) {
		
	    PdfPTable table  = new PdfPTable(14);
	    table.setWidthPercentage(100);

	    // Cell with 
	    PdfPTable matrix  = getTotalTable(statement);

	    // HEADER Cell Left
        PdfPCell cell = new PdfPCell(matrix);
        cell.setColspan(4);
        cell.setPaddingTop(5);
        cell.setPaddingBottom(5);
        cell.setPaddingRight(15);
        cell.setPaddingLeft(15);
        cell.setBackgroundColor(header1_background);
        cell.setBorderColor(header1_background);
        cell.setUseDescender(true);
        
	    // HEADER Cell Right
        PdfPCell cell2 = new PdfPCell();
        cell2.setColspan(10);
        cell2.setPaddingTop(5);
        cell2.setPaddingBottom(5);
        cell2.setPaddingRight(15);
        cell2.setPaddingLeft(15);
        cell2.setBackgroundColor(header2Background);
        cell2.setBorderColor(header2Background);
        cell2.setUseDescender(true);

        String bigHeading = statement.getStartDate().toString(monthyearFmt)+" Statement";
        
        Paragraph p4 = new Paragraph(bigHeading);
        p4.setFont(h1_inverse_font);
        p4.setAlignment(Element.ALIGN_LEFT);
        p4.setSpacingBefore(5);
        p4.setSpacingAfter(5);
        cell2.addElement(p4);
        
        // Header cell right - Partner info
        Paragraph p2 = new Paragraph("Prepared for "+statement.getCustomerName());
        p2.add(Chunk.NEWLINE);
        p2.add("Statement #:    "+statement.getBillNo());
        p2.add(Chunk.NEWLINE);
        p2.setFont(normal_inverse_font);
        p2.setAlignment(Element.ALIGN_LEFT);
        cell2.addElement(p2);

        // Add 2 column header cells
        table.addCell(cell2);
        table.addCell(cell);
        
        // final spacing
        p2.add(Chunk.NEWLINE);
	    table.completeRow();
	    return table;
    
	}

	
	public PdfPTable getLegalInformation () {

        PdfPTable table  = new PdfPTable(1);
        table.setWidthPercentage(100);

        table.getDefaultCell().setBackgroundColor(BaseColor.WHITE);
 
        Paragraph addr = new Paragraph();
        addr.setFont(legal_font);
        addr.setAlignment(Element.ALIGN_CENTER);
        addr.add("For questions concerning this Statement call us at (999) 999-4007");
        addr.add(Chunk.NEWLINE);
        addr.add("Monday through Friday - 4AM to 10PM PST");
        addr.add(Chunk.NEWLINE);
        addr.add("Saturday and Sunday - 6AM to 3PM PST");
        addr.add(Chunk.NEWLINE);
        addr.add(Chunk.NEWLINE);
 
        
        PdfPCell cell2 = new PdfPCell();
        cell2.setColspan(10);
        cell2.setPaddingTop(5);
        cell2.setPaddingBottom(5);
        cell2.setPaddingRight(15);
        cell2.setPaddingLeft(15);
        cell2.setBorderColor(BaseColor.GRAY);
        cell2.setBorderColorTop(BaseColor.BLUE);
        cell2.addElement(addr);
        
         table.addCell(cell2);
        
         return table;
	}
	    
	

	/*
	 * 
	 * Statement Summary
	 */
	public PdfPTable getStatementSummaryTable (Statement statement) {
		// currency
		Locale loc = Locale.forLanguageTag(statement.getLanguage());
		NumberFormat fmt = NumberFormat.getCurrencyInstance(loc);
		Currency cur = Currency.getInstance(statement.getCurrencyCode());
		fmt.setCurrency(cur);
        
		// numbers
        BigDecimal grossCharge  = statement.getGrossCharge();
        BigDecimal adjustments = statement.getAdjustments();
        BigDecimal finalCharge  = statement.getFinalCharge();
          
        // dates
        DateTime startD = statement.getStartDate();
        DateTime aDate = statement.getEndDate();
        if (startD == null ) startD= new DateTime(); // just in case - this will never happen
        if (aDate == null ) aDate= new DateTime();
        DateTime endD = aDate.minusDays(1);
       
        // build the cell
        int columnCount = 11;
        
        PdfPTable table  = new PdfPTable(columnCount);
        table.setWidthPercentage(100);
        
        /*
         * heading
         */
        table.getDefaultCell().setBackgroundColor(BaseColor.WHITE);
        table.getDefaultCell().setBorderColor(tbl_hdr_background);
        table.addCell(getHeadingCell("Period Start", 2, Element.ALIGN_CENTER));
        table.addCell(getHeadingCell("Period End", 2, Element.ALIGN_CENTER));
    	table.addCell(getHeadingCell("Item Subtotal",  2, Element.ALIGN_CENTER));
    	table.addCell(getHeadingCell("Adjustments", 2, Element.ALIGN_CENTER));
        table.addCell(getHeadingCell("Total Amount",  3, Element.ALIGN_CENTER));
        table.completeRow();

        /*
         * data
         */
        table.getDefaultCell().setBackgroundColor(BaseColor.WHITE);
        table.getDefaultCell().setBorderColor(BaseColor.WHITE);
        Font normal_font = new Font(FontFamily.HELVETICA, 7);
        normal_font.setColor(BaseColor.BLACK);
        table.addCell(getItemCell(startD.toString(dateFormat),  2, Element.ALIGN_CENTER));
        table.addCell(getItemCell(endD.toString(dateFormat),  2, Element.ALIGN_CENTER));
        table.addCell(getItemCell(fmt.format(grossCharge),  2, Element.ALIGN_RIGHT));
        table.addCell(getItemCell(fmt.format(adjustments),  2, Element.ALIGN_RIGHT));
        table.addCell(getItemCell(fmt.format(finalCharge),  3, Element.ALIGN_RIGHT));

        table.completeRow();
        return table;
	}
	
	    
	/*
	 * Statement items list
	 */
	public PdfPTable getStatementItemTable (Statement statement) {
		
		java.util.List<StatementItemBalance> statementItems = statement.getItemLines();
		if (statementItems == null || statementItems.size() == 0) {
			return null;
		}
		
		Locale loc = Locale.forLanguageTag(statement.getLanguage());
		NumberFormat currency = NumberFormat.getCurrencyInstance(loc);
		Currency cur = Currency.getInstance(statement.getCurrencyCode());
		currency.setCurrency(cur);
 		

		int columnCount = 10;

        
        /*
         * heading
         */
        PdfPTable itable  = new PdfPTable(columnCount);
        itable.setWidthPercentage(100);
       
        itable.getDefaultCell().setBackgroundColor(BaseColor.WHITE);
        itable.getDefaultCell().setBorderColor(BaseColor.BLUE);
        itable.addCell(getHeadingCell("Name",  3, Element.ALIGN_CENTER));
        itable.addCell(getHeadingCell("Quantity",  3, Element.ALIGN_CENTER));
        itable.addCell(getHeadingCell("Rate",  2, Element.ALIGN_CENTER));
        itable.addCell(getHeadingCell("Subtotal",  2, Element.ALIGN_CENTER));
        itable.completeRow();

        /*
         * items
         */
        itable.getDefaultCell().setBackgroundColor(BaseColor.WHITE);
        itable.getDefaultCell().setBorderColor(BaseColor.WHITE);

        DecimalFormat ratefmt = new DecimalFormat("0.000000"); 
       
        
        for (StatementItemBalance item : statementItems ) {

           	if (
	           	(item.getQuantity() == null || BigDecimal.ZERO.compareTo(item.getQuantity()) == 0 ) &&
	          	(item.getRatedTotal() == null || BigDecimal.ZERO.compareTo(item.getRatedTotal()) == 0 ) 
          	) {
           		continue;
           	}
         	
        	
        	BigDecimal quantity = (item.getQuantity() == null ) ? BigDecimal.ZERO : item.getQuantity();
        	BigDecimal ratedTotal = (item.getRatedTotal() == null ) ? BigDecimal.ZERO : item.getRatedTotal();
        	BigDecimal ratedUsed = (item.getRateUsed() == null ) ? BigDecimal.ZERO : item.getRateUsed();

        	itable.addCell(getItemCell(item.getDisplayName(),  3, Element.ALIGN_LEFT));
	        itable.addCell(getItemCell(ratefmt.format(quantity),  3, Element.ALIGN_RIGHT));
	        itable.addCell(getItemCell(ratefmt.format(ratedUsed),  2, Element.ALIGN_RIGHT));
	        itable.addCell(getItemCell(currency.format(ratedTotal),  2, Element.ALIGN_RIGHT));
	        itable.completeRow();
        }
        
        return itable;

		
	}
	    
	
	/*
	 * 
	 * Utilities and supporting 
	 * 
	 * 
	 */
	public PDFStatementFormatter()  {
		super();
		

		h1_inverse_font.setColor(tbl_hdr_font_color); // h1 inverse font
		h1_inverse_font.setStyle(Font.BOLD);

		h1_normal_font.setColor(header1_font); // h1 inverse font
		h1_normal_font.setStyle(Font.BOLD);

		normal_strong_font.setColor(BaseColor.DARK_GRAY); // h1 inverse font
		normal_strong_font.setStyle(Font.BOLD);

		legal_font.setStyle(Font.ITALIC);
		
		h2_inverse_font.setColor(tbl_hdr_font_color); // h2 inverse font

		normal_font.setColor(tbl_cell_font_color); // normal font
		
		
		normal_inverse_font.setColor(tbl_hdr_font_color); // normal font

		tbl_hdr_font.setColor(tbl_hdr_font_color);

	}
	
	
	/**
     * Creates a PdfPCell with the name of the month
     * @param calendar a date
     * @param locale a locale
     * @return a PdfPCell with rowspan 7, containing the name of the month
     */
    public PdfPCell getHeadingCell(String stringVal, int colSpan, int alignment) {
        PdfPCell cell = new PdfPCell();
        if (colSpan > 1) {cell.setColspan(colSpan);}
        
        cell.setLeading(0f, 0.2f);
        cell.setPaddingTop(0);
        
//        cell.setPaddingTop(col_pad_vertical);
        cell.setPaddingBottom(col_pad_vertical);
        cell.setPaddingRight(col_pad_horiz);
        cell.setPaddingLeft(col_pad_horiz);
        cell.setBackgroundColor(tbl_hdr_background);
        cell.setBorderColor(tbl_borderColor);
        cell.setUseDescender(true);
        Paragraph p = new Paragraph(stringVal);
        p.setFont(tbl_hdr_font);
        p.setAlignment(alignment);
        cell.addElement(p);
        return cell;
    }
    
	/**
     * Creates a PdfPCell with the name of the month
     * @param calendar a date
     * @param locale a locale
     * @return a PdfPCell with rowspan 7, containing the name of the month
     */
    public PdfPCell getDataCell(String stringVal, int colSpan, int alignment) {
        PdfPCell cell = new PdfPCell();
        if (colSpan > 1) {cell.setColspan(colSpan);}
        cell.setPaddingTop(0);
        cell.setPaddingBottom(0);
        cell.setPaddingRight(col_pad_horiz);
        cell.setPaddingLeft(col_pad_horiz);
        cell.setBackgroundColor(tbl_cell_background);
        cell.setBorderColor(tbl_borderColor);
        cell.setUseDescender(true);
        Paragraph p = new Paragraph(stringVal);
        p.setFont(item_font);
        p.setAlignment(alignment);
        cell.addElement(p);
        return cell;
    }
  
	/**
     * Creates a PdfPCell with the name of the month
     * @param calendar a date
     * @param locale a locale
     * @return a PdfPCell with rowspan 7, containing the name of the month
     */
    public PdfPCell getItemCell(String stringVal, int colSpan, int alignment) {
        PdfPCell cell = new PdfPCell();
        if (colSpan > 1) {cell.setColspan(colSpan);}
        cell.setPaddingTop(0);
        cell.setPaddingBottom(0);
        cell.setPaddingRight(col_pad_horiz);
        cell.setPaddingLeft(col_pad_horiz);
        cell.setBackgroundColor(tbl_cell_background);
        cell.setBorderColor(tbl_borderColor);
        cell.setUseDescender(true);
        Paragraph p = new Paragraph(stringVal);
        p.setFont(item_font);
        p.setAlignment(alignment);
        cell.addElement(p);
        return cell;
    }
       
    
	/**
     * Creates a PdfPCell with the name of the month
     * @param calendar a date
     * @param locale a locale
     * @return a PdfPCell with rowspan 7, containing the name of the month
     */
    public PdfPCell getDataCell(String stringVal, int colSpan, int alignment, Font font) {
        PdfPCell cell = new PdfPCell();
        if (colSpan > 1) {cell.setColspan(colSpan);}
        cell.setPaddingTop(0);
        cell.setPaddingBottom(0);
        cell.setPaddingRight(col_pad_horiz);
        cell.setPaddingLeft(col_pad_horiz);
        cell.setBackgroundColor(tbl_cell_background);
        cell.setBorderColor(tbl_borderColor);
        cell.setUseDescender(true);
        Paragraph p = new Paragraph(stringVal);
        p.setFont(item_font);
        p.setAlignment(alignment);
        cell.addElement(p);
        return cell;
    }
   
    public Image getLogo(String imagefile, float scale) throws BadElementException, MalformedURLException, IOException {
		BufferedImage bufferedImage =ImageIO.read( ClassLoader.getSystemResource(imagefile) );
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(bufferedImage, "png", baos);
		Image image1 = Image.getInstance(baos.toByteArray());
		image1.scalePercent((float) 25);
    	return image1;
    }
 
	/**
	 *  
	 *  Inner CLASS to add a header and a footer.
	 *  
	 */
	class HeaderFooter extends PdfPageEventHelper {
	    /** Alternating phrase for the header. */
	    /** Current page number (will be reset for every chapter). */
	    int pagenumber  = 0;

	    /**
	     * Initialize one of the headers.
	     * @see com.itextpdf.text.pdf.PdfPageEventHelper#onOpenDocument(
	     *      com.itextpdf.text.pdf.PdfWriter, com.itextpdf.text.Document)
	     */
	    public void onOpenDocument(PdfWriter writer, Document document) {
//	        header[0] = new Phrase("Movie history");
	    }

	    /**
	     * Initialize one of the headers, based on the chapter title;
	     * reset the page number.
	     * @see com.itextpdf.text.pdf.PdfPageEventHelper#onChapter(
	     *      com.itextpdf.text.pdf.PdfWriter, com.itextpdf.text.Document, float,
	     *      com.itextpdf.text.Paragraph)
	     */
	    public void onChapter(PdfWriter writer, Document document,
	            float paragraphPosition, Paragraph title) {
//	        header[1] = new Phrase(title.getContent());
	        pagenumber = 1;
	    }

	    /**
	     * Increase the page number.
	     * @see com.itextpdf.text.pdf.PdfPageEventHelper#onStartPage(
	     *      com.itextpdf.text.pdf.PdfWriter, com.itextpdf.text.Document)
	     */
	    public void onStartPage(PdfWriter writer, Document document) {
	        pagenumber++;
	    }

	    /**
	     * Adds the header and the footer.
	     * @see com.itextpdf.text.pdf.PdfPageEventHelper#onEndPage(
	     *      com.itextpdf.text.pdf.PdfWriter, com.itextpdf.text.Document)
	     */
	    public void onEndPage(PdfWriter writer, Document document)
	    {
	        Rectangle rect = writer.getBoxSize("art");
	        rect.setBorder(1);
	        
	        ColumnText colText = new ColumnText(writer.getDirectContent());

	        Phrase legal = new Phrase();
	        legal.setFont(normal_font);
	        legal.add("For questions concerning this Statement call us at (800) 888-8888\nMonday through Friday - 8AM to 10PM PST\nSaturday and Sunday - 6AM to 3PM PST");
	        colText.setSimpleColumn(
	        		legal, 
	        		rect.getLeft() + 18, 
	        		rect.getBottom() + 18, 
	        		rect.getLeft() + rect.getWidth(), 
	        		rect.getBottom() - 532, 
	        		11, 
	        		Element.ALIGN_CENTER);
	        
	        try {
				colText.go();
			} catch (DocumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
	    }	
	}	
	
	
	
	
 	
}
