import json
import re
import time
import hashlib
from datetime import datetime
from optparse import OptionParser

class Building:
    #
    #
    def __init__(self, baseName, buildingMap):
        self.baseName = baseName
        self.buildingName="unknown"
        self.installation = "unknown"
        self.totalbeds=-1
        self.trainingbeds=-1
        self.permanentbeds=-1
        self.transientbeds=-1
        self.errors = []
        self.warnings=[]
        self.tuples = []
        self.fieldcount=0
        self.tuples.append('"version_date" : "%s"' % (datetime.today().strftime('%Y-%m-%d')))
        self.tuples.append('"archive_flag" : 0')
        self.mapBuilding(buildingMap)

        if (self.totalbeds == -1):
            self.totalbeds=0
            self.tuples.append('"total_beds" : 0');
        if (self.transientbeds == -1):
            self.transientbeds=0
            self.tuples.append('"transient_beds" : 0');
        if (self.trainingbeds == -1):
            self.trainingbeds=0
            self.tuples.append('"training_beds" : 0');
        if (self.permanentbeds == -1):
            self.permanentbeds=0
            self.tuples.append('"permanent_beds" : 0');

        if (self.totalbeds != (self.trainingbeds+self.permanentbeds+self.transientbeds)):
            self.warnings.append("total_beds does not equal sum of transient+permanent+training beds")

        id = '%s-%s-%s' % (self.baseName, self.buildingName,datetime.today().strftime('%Y-%m-%d'))
        self.tuples.append('"id" : "%s"' % id)
        self.tuples.append('"hashkey" : "%s"' % id)
        if (self.baseName == "unknown" and self.installation != "unknown"):
            self.baseName =  self.installation
        elif  (self.installation == "unknown"):
            self.tuples.append('"base" : "%s"' % self.baseName)

    #
    #
    def mapBuilding(self, buildingMap):
        for k, y in buildingMap.items():
            self.mapFieldName(k,y)

    #
    #
    def mapFieldName(self, name, value):
        if value == None:
            return
        self.fieldcount +=1
        testname = name.lower()
        installation = self.baseName
        if (testname.startswith('installation')):
            self.installation = value
            self.tuples.append('"base" : "%s"' % value);
        elif (testname.startswith('phase')):
            self.tuples.append('"phase" : "%s"' % value);
        elif (testname.startswith('area')):
            self.tuples.append('"area" : "%s"' % value);
        elif (testname.startswith('notes')):
            self.tuples.append('"notes" : "%s"' % self.strip_non_ascii(value));
        elif (testname.startswith('contract')):
            self.tuples.append('"contract" : "%s"' % value);
        elif (testname == 'acronym'):
            self.tuples.append('"base_id" : "%s"' % value);
        elif (testname.startswith('location') ):
            self.buildingName = value;
            self.tuples.append('"building" : "%s"' % value);
        elif ('transient' in testname ):
            self.transientbeds = self.toInt(value)
            self.tuples.append('"transient_beds" : %s' % self.transientbeds);
        elif ('permanent' in testname ):
            self.permanentbeds = self.toInt(value)
            self.tuples.append('"permanent_beds" : %s' % self.permanentbeds);
        elif ('training' in testname ):
            self.trainingbeds = self.toInt(value)
            self.tuples.append('"training_beds" : %s' % self.trainingbeds );
        elif ('beds' in testname ):
            self.totalbeds = self.toInt(value)
            self.tuples.append('"total_beds" : %s' % self.totalbeds);
        elif (testname.startswith('launch') ):
            self.tuples.append('"launch_date" : "%s 00:00:00"' % value);
        elif ("out of service" in testname ):
            v = self.toOutofServiceDate(value)
            if (v != None):
               self.tuples.append('"out_of_service" : "%s"' % v);
        elif (testname.startswith('notes')  ):
            self.notes = value
        else :
           self.fieldcount -=1
           self.errors.append("bad field name %s (%s)" % (testname,value))

    #
    #
    def toInt(self,value):
        try:
            return int(value)
        except :
            return 0
    #
    #
    def strip_non_ascii(self,string):
        stripped = (c for c in string if 31 < ord(c) < 127)

        return ''.join(stripped).replace('"', "'")



    #
    #
    def toOutofServiceDate(self,value):
        try:
            datetime_object = datetime.strptime(value, '%m/%d/%y')
            return datetime_object.strftime('%Y-%m-%d 00:00:00')
        except ValueError:
            return None


    #
    #
    def toJson(self):
        return '{%s}' % (','.join(self.tuples))