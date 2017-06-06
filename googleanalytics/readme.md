

# google analytics reader
A simple python app to read a few different google analytics reports and get them into a format
that can be loaded into a database.

# motivation
It's sometimes helpful to include google analtics data to fill out a dashboard or even join to internal data.

# contents
* gaMain.py:  The main program and all flow logic
* gaRecord.py: an object representing a single record -- and array of these is a report
* gaAnalyticsApiV4.py: A modified example of the google analytics documentation for reading a report.  You should start there first.
* client_secrets.json: a google anylytic service key -- you get this from google and would place it somewhere safe.  I just stored it with the program for simplicity 
