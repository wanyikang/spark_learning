# -*- coding: utf-8 -*-
from pyspark import SparkContext
import sys
import re
import shutil

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
    pass

def load_call_sign_table():
    f = open(file_dir + '/files/callsign_tbl_sorted', 'r')
    return f.readlines()

if __name__ == '__main__':
    validating_callsign(file)

