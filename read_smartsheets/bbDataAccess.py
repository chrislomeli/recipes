import psycopg2
import logging
import datetime
import logging
import bbConfig
import boto3
from datetime import timedelta

conn_string = "dbname='%s' port='%s' user='%s' password='%s' host='%s'" % (bbConfig.RED_DB,bbConfig.RED_PORT,bbConfig.RED_USER,bbConfig.RED_PASS,bbConfig.RED_HOST)
CONNECTION = psycopg2.connect(conn_string);

"""
---------------------------------------------
connect_to_rs
    connect to redshift
---------------------------------------------
"""
def closeData():
    if CONNECTION != None:
        CONNECTION.close()

    return

"""
---------------------------------------------
connect_to_rs
    connect to redshift
---------------------------------------------
"""
def initData():
    try:
        conn_string = "dbname='%s' port='%s' user='%s' password='%s' host='%s'" % (bbConfig.RED_DB,bbConfig.RED_PORT,bbConfig.RED_USER,bbConfig.RED_PASS,bbConfig.RED_HOST)
        CONNECTION = psycopg2.connect(conn_string);
        CONNECTION.set_session(autocommit=True)
        logging.info('Redshift Connection Successful')

    except psycopg2.Error as e:
        logging.exception(e)
        exit(1)
    return

"""
---------------------------------------------
upload_localfile
    * upload a file to s3
---------------------------------------------
"""
def upload_localfile(table_name, base_filename, fullpath):
    key = "%s/%s/%s/%s" % (bbConfig.S3_PREFIX, table_name, datetime.datetime.today().strftime("%Y-%m-%d"),base_filename)
    logging.info("UPLOAD %s to %s:%s ..." % (fullpath, bbConfig.S3_BUCKET, key))
    try :
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(fullpath, bbConfig.S3_BUCKET, key)
        return "s3://%s/%s" % (bbConfig.S3_BUCKET, key)  ## todo test that this is the right format

    except Exception as e:
        logging.exception("FAILED to load %s to %s:%s error(%s)" % (fullpath, bbConfig.S3_BUCKET, table_name, e))

    return None



"""
---------------------------------------------
copy_s3file
    copy an s3 files to whatever table we are
    working on
---------------------------------------------
"""
def copy_s3file(table_name, from_s3_fn):
    if CONNECTION == None:
        initData()

    result=None
    sql="""copy %s.%s.%s from '%s' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' format as json 'auto'; commit;""" % (bbConfig.RED_DB,bbConfig.RED_SCHEMA, table_name, from_s3_fn, bbConfig.AWS_ACCESS_KEY_ID, bbConfig.AWS_SECRET_ACCESS_KEY)

    try:
        cur = CONNECTION.cursor()

        logging.info("truncating %s" % table_name)
        cur.execute("truncate table %s.%s" % (bbConfig.RED_SCHEMA,table_name))
        res = cur.execute(sql)
        cur.close()
        result=True
    except psycopg2.Error as e:
        print (e.pgerror)
        logging.exception (e.diag.message_detail)
    except Exception as e:
        logging.exception(e)
    finally:
        cur.close()
    return result
