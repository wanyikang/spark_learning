# -*- coding: utf-8 -*-
from pyspark import SparkContext
import sys
import re
import shutil
import bisect
import urllib3
import json

outputDir = sys.argv[1]
file_dir = './file'

sc = SparkContext('local', appName='ChapterSixExample')
file = sc.textFile(file_dir + '/callsigns')


count = sc.accumulator(0)
def count_lines(fileRdd):
    """ Count lines with KK6JKQ using accumulators."""
    fileRdd.foreach(incrementCounter)
    print('Lines with KK6JKQ {0:d}'.format(count.value))

def incrementCounter(line):
    global count  # access the counter
    if 'KK6JKQ' in line:
        count += 1


blankLines = sc.accumulator(0)
def accumulate_empty_line(fileRdd):
    """ Count empty lines through accumulator."""
    shutil.rmtree(outputDir)
    callsigns = fileRdd.flatMap(extractCallSigns)
    callsigns.saveAsTextFile(outputDir + 'not_empty')
    print('blank lines: {0:d}'.format(blankLines.value))

def extractCallSigns(line):
    global blankLines
    if line == '':
        blankLines += 1
        return ()
    return line.split(' ')


valid_sign_count = sc.accumulator(0)
invalid_sign_count = sc.accumulator(0)
def validating_callsign(fileRdd):
    """ Validating callsigns through accumulator.
    NOTE: write accumulator in function used in transform, that shloud be
    avoided in product.
    """
    valid_signs = fileRdd.flatMap(extractCallSigns).filter(validate_sign)
    non_duplicate_signs = valid_signs.map(lambda sign: (sign, 1)).reduceByKey(
            lambda x, y: x+y)
    # force evaluation so the accumulators are populated
    non_duplicate_signs.count()
    total = invalid_sign_count.value + valid_sign_count.value
    if invalid_sign_count.value > total * 0.5:
        print('Too many errors {0:d} in {1:d}'.format(invalid_sign_count.value,
            total))
    else:
        print('validate done!')
        shutil.rmtree(outputDir)
        non_duplicate_signs.saveAsTextFile(outputDir + '/valid_signs')
    return non_duplicate_signs

def validate_sign(sign):
    global valid_sign_count, invalid_sign_count
    if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
        valid_sign_count += 1
        return True
    else:
        invalid_sign_count += 1
        return False


# Helper functions for looking up the call signs
def lookup_country(sign, prefixes):
    pos = bisect.bisect_left(prefixes, sign)
    return prefixes[pos].split(',')[1]

def load_call_sign_table():
    f = open(file_dir + '/callsign_tbl_sorted', 'r')
    return f.readlines()

sign_prefixes = sc.broadcast(load_call_sign_table())

def process_sign_count(sign_count):
    country = lookup_country(sign_count[0], sign_prefixes.value)
    return (country, sign_count[1])

def count_country(non_duplicate_signs):
    country_count = non_duplicate_signs.map(process_sign_count).reduceByKey(
        lambda x, y: x + y)
    shutil.rmtree(outputDir)
    country_count.saveAsTextFile(outputDir + '/country_count')


def processCallSigns(signs):
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

def fetchCallSigns(fileRdd):
    """Fetch call signs"""
    valid_signs = fileRdd.flatMap(extractCallSigns).filter(validate_sign)
    sign_jsons = valid_signs.mapPartitions(lambda callSigns: processCallSigns(
        callSigns))
    shutil.rmtree(outputDir)
    sign_jsons.saveAsTextFile(outputDir + '/sign_jsons')

if __name__ == '__main__':
    fetchCallSigns(file)
