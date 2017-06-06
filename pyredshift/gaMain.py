import boto3
import psycopg2
import datetime
from datetime import timedelta
import gaConfig
import os.path


conn_string = "dbname='%s' port='%s' user='%s' password='%s' host='%s'" % (gaConfig.RED_DB,gaConfig.RED_PORT,gaConfig.RED_USER,gaConfig.RED_PASS,gaConfig.RED_HOST)
CONNECTION = psycopg2.connect(conn_string);


"""
---------------------------------------------
copy_s3file
    copy an s3 files to whatever table we are
    working on
---------------------------------------------"""
def copy_s3file(table_name, from_s3_fn):

    result=None
    sql="""copy %s.%s.%s from '%s' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' format as json 'auto'; commit;""" % (gaConfig.RED_DB,gaConfig.RED_SCHEMA, table_name, from_s3_fn, gaConfig.AWS_ACCESS_KEY_ID, gaConfig.AWS_SECRET_ACCESS_KEY)

    try:
        cur = CONNECTION.cursor()
        res = cur.execute(sql)
        cur.close()
        result=True
    except psycopg2.Error as e:
        print (e.pgerror)
        print(e.diag.message_detail)
    except Exception as e:
        print(e)
    finally:
        cur.close()
    return result


"""
---------------------------------------------
upload_localfile
    * upload a file to s3
---------------------------------------------"""
def upload_localfile_to_s3(fullpath, bucket, key):
    print("UPLOAD %s to %s:%s ..." % (fullpath, bucket, key))
    try :
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(fullpath, bucket, key)
        return "s3://%s/%s" % (bucket, key)

    except Exception as e:
        print("FAILED to load %s to %s:%s error(%s)" % (fullpath, gaConfig.S3_BUCKET, key, e))

    return None


"""
---------------------------------------------
load_to_redshift
- given a local director, a file name in that directory, and a redshift tablename
- load the file into redshift

notes:
- this is a json example.  It's advantage is that you don't work abou the order of the columns
- but it's slower than the csv version
---------------------------------------------"""
def load_to_redshift (localdir, localfilename, redshift_table):
    # files are organized by s3 buckets
    bucket = gaConfig.S3_BUCKET

    # you can place fles anywhere in the bucket
    # but in this example we to organize our 'folders' by the topic/yyyy-mm-dd
    # for example mybucket::/my_topic/2017-01-31/myfile.json
    # you will also see YYYY MM DD in separate folders for partitioning into hive:  mybucket::/my_topic/2017/01/31/myfile.json
    # it's a preference, but in many cases a single sortable partition key is easier to work with in hive, and redshift does not care
    key = "%s/%s/%s/%s" % (gaConfig.S3_PREFIX, redshift_table, datetime.datetime.today().strftime("%Y-%m-%d"),localfilename)

    # constructing the full pathname
    fullpath = "%s/%s" % (localdir, localfilename)

    # upload the file to s3 - you need to get a single url string back that is in the formate preferred by redshift
    s3file = upload_localfile_to_s3(fullpath, bucket, key)

    # use the redshift copy command to copy the files in
    # this is ok for light loads - for heavier loads you wan to create a lot of files for upload
    #  and list them into a "manifest" file that you also upload to s3
    #  then you call the redshift copy command  with the manifest keyword and the manifest files instead of the individual file names
    copy_s3file(redshift_table, s3file)


"""
---------------------------------------------
main
---------------------------------------------
"""
if __name__ == '__main__':
    load_to_redshift("/mylocaldirectory", "myfile.json", "my_redshift_table")
