## excel sql workbook
A utility to take several sql statements with date arguments and dump the output to an Excel workbook using apache poi.

## motivation
Normally this is something your reporting tool would do.  Once in a while we run into a situation were we just want to dump a set of summaries to a user
in excel - a little nicer than just csv.  In this example we dump a lot of monthly queries at month-end, so this utility is coded to handle a date argument.
 
## contents
* com.temp.sql: each sql statement is a separate class and there is a simple dao class to connect directly to jdbc
** SQL****.java - each sql class represents a sql query
* com.temp.worksheet:
** SQLWorkbookApp: main function 
** SQLWorkbookService: the real worker
** SQLStatementAdapter: parent class for generalizing the sql classes
** SQLWorkbookApp: workbook creation
** SQLWorkbook: representation of the apache poi worksheets



