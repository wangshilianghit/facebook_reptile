#!/usr/bin/python

from urllib2 import urlopen, URLError
import ujson as json
import time
from httplib import BadStatusLine
import sys, os
import time
import gzip
from argparse import ArgumentParser

"""
    This is the info from the bogus app I used
"""
app_id = '653943651304297'
app_secret = '1bc923adced1393d7565dca05a0ea24e'
access_token = urlopen("https://graph.facebook.com/oauth/access_token?client_id="+app_id+"&client_secret="+app_secret+"&grant_type=client_credentials").read()
print 'access toekn: ' + str(access_token)

urlQuery = 'https://graph.facebook.com/search?type=post'

MAX_ERRORS = 10   # how many times to retry something before existing
MAX_TRIES = 5
ERROR_PAUSE = 5   # how manay seconds to pause after an error

def readJSONUrl(url):
    """reads a url aurlopennd interprits the results using JSON"""
    count = 0;
    while (True):
        try:
            print "trying " + url + '\n'
            return json.loads(urlopen(url).read())
            break

        except (URLError, BadStatusLine), e:
            count += 1
            if (count < MAX_ERRORS): 
                print "      ERROR: " + str(sys.exc_info()[0]) + ", " + str(e) + ", " + str(count) + " try, trying again in " + str(ERROR_PAUSE) + " seconds"
                print e.message
                time.sleep(ERROR_PAUSE)
            else:
                print "URL ERROR: too many tries, exiting"
                sys.exit(1);
                

def convertTimeToInt(timestr):
    """converts from format Year-month-day to second from epoch"""
    return int(time.mktime(time.strptime(timestr, '%Y-%m-%dT%H:%M:%S')))


def convertIntToTime(tint):
    """converts from second from epoch to format Year-month-dayThour:min:sec"""
    return str(time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(tint)))

"""
    stripPlusTime
    strip the time after +
"""
def stripPlusTime(timestr):

    return timestr.rstrip('0123456789').rstrip('+')

"""
    decreateTime: 
    Decrease the time by 1 second
"""
def decreaseTime(timestr):

    newTime = convertIntToTime(convertTimeToInt(timestr)-1)
    return newTime

"""
    readSerachTerm:
    Node: In this function, it only supports the following file foramt for keywords:
    start with ***=keyword
    keyword is separated with ,
    I will replace the space character with + character
"""
def readSearchTerm(filename):

    try:
        if filename == 'the':
            return ['the']

        infile = open(filename, 'r')

        line = infile.readlines()[0]
        keywords = line.strip().split(',')
        keywords[0] = keywords[0].split('=')[1]
        
        keywords = [keyword.replace(' ', '+') for keyword in keywords]

        return keywords

    except IOError:
        sys.stderr.write('Error: unable to open file %s\n' % filename)
        infile.close()
        sys.exit(-1)

    else:
        sys.stderr.write('Error: error in processing file %s\n' % filename)
        infile.close()
        sys.exit(-1)
    
def getFileName(output):

    if output.rfind('/') == -1:
        directory = time.strftime("%Y/%m/")
        file_name = directory + time.strftime("%Y_%m_%d") + '.json.gz'

    else:
        directory = output[0: output.rfind('/') + 1] + time.strftime("%Y/%m/")
        file_name = directory + time.strftime("%Y_%m_%d") + '.json.gz'

    if not os.path.exists(directory):
        os.makedirs(directory)

    return file_name


def main():

    parser = ArgumentParser(description='Store the facebook data based on the keywords from the file.')
    parser.add_argument("-f", "--keyword_file", type=str, help="Input the keyword file after -f")
    parser.add_argument("-o", "--output", type=str, help="Input the output path after -f")
    parser.add_argument("-n", "--max_number", type=int, help="The maximum number of post to return after -n")

    options = parser.parse_args()

    search_file_name = options.keyword_file
    if search_file_name == None:
        parser.print_help()
        exit(1)
    keywords = readSearchTerm(search_file_name)

    output = options.output
    file_name = getFileName(output)
     
    max_number = options.max_number
    if max_number == None:
        max_number = 1000000000 
    
    since = '2012-01-01' 
    until = None 

    lastResults = 1
    totalResults = 0

    for keyword in keywords:
        while totalResults < max_number:

            addParms = '&q=' + keyword
            if (since): addParms += '&since=' + str(since)
            if (until): addParms += '&until=' + str(until)
            addParms += '&' + access_token
            print "URL QUERY: " + urlQuery + addParms

            results = readJSONUrl(urlQuery + addParms)['data']

            gzip_out = gzip.open(file_name, 'ab+')

            for result in results:
                json_object = json.dumps(result)
                gzip_out.write(json_object)
                gzip_out.write('\n')

            gzip_out.close()

            lastResults = len(results)
            totalResults += lastResults
            print 'got results: ' + str(lastResults) + "   Total: " + str(totalResults)

            if lastResults:

                earLiestTime = results[-1]['created_time']
                print 'earliest time: ' + str(earLiestTime)
                earLiestTime = stripPlusTime(earLiestTime)
                print 'earliest time: ' + str(earLiestTime)
                until = decreaseTime(earLiestTime)
                count = 0

            else:

                count += 1
                if (count < MAX_TRIES): 
                    print "      ERROR: " + str(sys.exc_info()[0]) + ", " + str(e) + ", " + str(count) + " try, trying again in " + str(ERROR_PAUSE) + " seconds"
                    print e.message
                    time.sleep(ERROR_PAUSE)
                else:
                    print "Get Results Error: too many tries, exiting"
                    sys.exit(1);



if __name__ == "__main__":

    main()

