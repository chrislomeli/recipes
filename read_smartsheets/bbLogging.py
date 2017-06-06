from timeit import default_timer as timer
import logging
import os

#
# set up any globals
#
job_start = timer()


#
# create a properties object so we cna change them later
#  without changing in the code
#
defaultLogConfig = {
                  'application_name':'generic_app',
                  'log_format' : '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                  'logging_path':'/temp/redshift',
                  'logging_file':'test'+ str(int(job_start)) + '.txt',
                  'logging_level':logging.INFO,
                  'console' : True
                }

#############################################
# Setup logging
#
def setupLogging(conf=defaultLogConfig):

    logFormatter = logging.Formatter(conf['log_format'])
    rootLogger = logging.getLogger()
    rootLogger.setLevel(conf['logging_level'])
    if not os.path.exists(conf['logging_path']):
        os.makedirs(conf['logging_path'])

    fileHandler = logging.FileHandler("{0}/{1}.log".format(conf['logging_path'], conf['logging_file']))
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    if defaultLogConfig['console']:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        rootLogger.addHandler(consoleHandler)

    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)
    logging.getLogger('smartsheet.smartsheet').setLevel(logging.ERROR)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)

#####################################
# test
#
def main():
    setupLogging(defaultLogConfig)
    logging.info("\nJob Complete: Took {0} seconds".format(timer() - job_start))

if __name__ == "__main__":
    # execute only if run as a script
    main()
