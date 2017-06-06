import json
import re
import time
import hashlib

class gaRecord:

    def __init__(self, rec, day):
        self.tuples = []
        self.day = day
        self.type = type
        self.dimensions(rec)

    def dimensions(self, rec):
        self.hashkey = hashlib.sha224("k".encode('utf-8'))
        for key, value in rec.items():
            self.hashkey.update(value.encode('utf-8'))
            trash,newK = key.split(':')
            newK = newK.lower()
            self.tuples.append('"%s": "%s"' % (newK,value) )

        self.hashkey.update(str(self.day).encode('utf-8'))
        self.tuples.append('"hashkey": "%s"' % self.hashkey.hexdigest() )
        self.tuples.append( '"load_date":"'+time.strftime("%Y-%m-%d %H:%M:%S")+'"')
        self.tuples.append( '"event_date":"'+ str(self.day)+'"')


    def toJson(self):
        return '{%s}' % (','.join(self.tuples))