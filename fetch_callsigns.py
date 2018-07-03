# -*- coding: utf-8 -*-
from pyspark import SparkContext
import re
import bisect
import urllib3
import json
import shutil

file_dir = '/home/richard_wan/Projects/callsign'

def lookup_country(sign, prefixes):
    pos = bisect.bisect_left(prefixes, sign)
    return prefixes[pos].split(',')[1]

def load_call_sign_table():
    f = open(file_dir + '/callsign_tbl_sorted', 'r')
    return f.readlines()

def extract_callsigns(line):
    if line == '':
        return ()
    return line.split(' ')

def validate_sign(sign):
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        return True
    else:
        return False

def process_sign_count(sign_count):
    country = lookup_country(sign_count[0], sign_prefixes.value)
    return (country, sign_count[1])

def process_callsigns(signs):
    """Lookup call signs using a connection pool"""
    # Create a connection pool
    http = urllib3.PoolManager()
    # the URL associated with each call sign record
    urls = map(lambda x: "http://73s.com/qsos/%s.json" % x, signs)
    # create the requests (non-blocking)
    requests = map(lambda x: (x, http.request('GET', x)), urls)
    # fetch the results
    result = map(lambda x: (x[0], json.loads(x[1].data)), requests)
    # remove any empty results and return
    filtered = filter(lambda x: x[1] is not None, result)
    return filtered

def fetch_callsigns(fileRdd):
    valid_signs = fileRdd.flatMap(extract_callsigns).filter(validate_sign)
    sign_jsons = valid_signs.mapPartitions(lambda callsigns: process_callsigns(
        callsigns))
    shutil.rmtree(file_dir + '/output', ignore_errors=True)
    sign_jsons.saveAsTextFile(file_dir + '/output')

if __name__ == '__main__':
    sc = SparkContext('spark://10.0.11.30:7077', appName='callsigns')
    # golbal broadcast variable
    sign_prefixes = sc.broadcast(load_call_sign_table())
    frdd = sc.textFile(file_dir + '/callsigns')
    fetch_callsigns(frdd)

