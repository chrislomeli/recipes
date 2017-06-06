import datetime
from datetime import timedelta
import os.path
import gaAnalyticsApiV4


GA_REPORTS = ['GOOGLE_USER_LANGUAGE','GOOGLE_USER_DEVICE']
LOCAL_PATH = "c:\\temp"

"""
---------------------------------------------
do all reports in the GA_REPORTS table
 iterate through each report - foreach report:
---------------------------------------------"""
def download_all_reports(next_date):
    print("Create all reports "+str(next_date))
    for report in GA_REPORTS:
        download_report_days(report, next_date)

"""
---------------------------------------------
one_report
iterates through each day and prints a separate report for that day
---------------------------------------------"""
def download_report_days(subject, next_date):
    print("********* REPORTING %s from %s to %s **************" % (subject, next_date, next_date))
    while next_date <= datetime.date.today():
        load_google_report(subject, next_date)
        next_date = next_date + timedelta(days=1)

"""
---------------------------------------------
load_google_report
main program for downloading one report for one day
---------------------------------------------"""
def load_google_report(subject, day):

    # call the gaAnalytics downloader
    print("\tLOAD REPORT %s for %s " % (subject, day))
    # the local file name should be the same as the table with a date and json extension - we'll use this base name to create an s3 file
    base_filename = "%s-%s.json" % (subject, day.strftime("%Y-%m-%d"))

    # the directory where we will store local files is configured in gaConfig.LOCAL_PATH
    fullpath = "%s/%s" % (LOCAL_PATH, base_filename)

    try:
        # decide which report we are doing and call the right report function
        if subject == 'GOOGLE_USER_LANGUAGE':
            report=gaAnalyticsApiV4.get_language_report(day)
            write_json(report, fullpath, day)

        elif subject == 'GOOGLE_USER_DEVICE':
            report=gaAnalyticsApiV4.get_device_report(day)
            write_json(report, fullpath, day)

        else:
            createdFile = None

    except Exception as e:
       print(e)

    # check results
    if ( os.path.exists(fullpath)):
       print("\tfile %s created..." % base_filename)
    else:
       print("\tfile %s was not created for %s on %s" % (base_filename, subject, day))
       return;



"""
---------------------------------------------
write_json
---------------------------------------------
"""
def write_json(report, fullpath, day):

    try:
        with open(fullpath, 'w') as fp:
            for rec in report:
                fp.write(rec.toJson())
                fp.write("\n")

            fp.close()
    except Exception as e:
       print(e)

"""
---------------------------------------------
main
---------------------------------------------
"""
if __name__ == '__main__':
    download_all_reports(datetime.date.today())
