# pdf writer
A simple utility to demonstrate writing output to PDF format using itextpdf.  The example here is an invoice getting data from a postgres database.

## motivation
The requirement to convert to pdf does not happen often, but if you are standing up a data portal and need to go directly from data to PDF this is a way to accomplish the task.

* StatementApp: main program
* Statement:  model class for a single statement (invoice)
* StatementItemBalance:  model class for a single statement line item 
* PDFStatementFormatter:   given a Statement - write that statement to a PDF




