"""Hello Analytics Reporting API V4."""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from gaRecord import gaRecord
import datetime
from datetime import timedelta


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'client_secrets.json'
VIEW_ID = '136534408'

credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

ANALYTICS = build('analytics', 'v4', credentials=credentials)

def initialize_analyticsreporting():
  return ANALYTICS

"""
  All of the get_***_reports below are similar to the basic google documentation
  except that we are formatting the output into a dict object
  to provide an example of how one might use the output
"""
def get_device_report(day):
  analytics= initialize_analyticsreporting()
  response = analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{"startDate": str(day), "endDate": str(day)}],
          'metrics': [{'expression': 'ga:hits'},{'expression': 'ga:sessions'},{'expression': 'ga:avgSessionDuration'},{'expression': 'ga:Users'},{'expression': 'ga:newUsers'}],
          'dimensions': [{'name': 'ga:deviceCategory'},{'name': 'ga:operatingSystem'}]
        }]
      }
  ).execute()
  return response_formatter(response, day)

def get_language_report(day):
  analytics= initialize_analyticsreporting()
  response =  analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{"startDate": "2017-05-01", "endDate": "2017-05-21"}],
          'metrics': [{'expression': 'ga:hits'},{'expression': 'ga:sessions'},{'expression': 'ga:avgSessionDuration'},{'expression': 'ga:Users'},{'expression': 'ga:newUsers'}],
          'dimensions': [{'name': 'ga:language'}]
        }]
      }
  ).execute()
  return response_formatter(response, day)

"""
  response_formatter
  returns an array of gaRecord objects

  notes:
  This is an example of how you might process the output
  In this case we are unpacking the google response and placing them on an array
  - somthing you would not want to do with a large data set
  - just before appending to the array we are placing each row on a gaRecord() object
    to place all of the scenario specific logic in one place and to make our OO
    folks happy
"""
def response_formatter(response, day):
  final = []
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      # one record
      record = {}
      for header, dimension in zip(dimensionHeaders, dimensions):
        record[header] = dimension

      for i, values in enumerate(dateRangeValues):
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          record[metricHeader.get('name')] = value

      # append our record to the final array
      final.append(gaRecord(record, day))

  return final


