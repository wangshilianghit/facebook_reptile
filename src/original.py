#!/usr/bin/python

#####################################################
##PYTHON SCRIPT TO BOTH PULL DATA FROM FACEBOOK
##AND TO INTERACT WITH THE DATA
##
## andy.schwartz@gmail.com - Summer 2011
## Updated:
## -Lukasz Dziurzynski
## -Maarten Sap
##
##TODO:
## -add simplejson to the repository
## -sort terms for by existing frequencies of terms (starting with most frequent) in ordre to take best advantage of paral.

from urllib2 import urlopen, URLError
from optparse import OptionParser
import math
import sys,os
import re
import pprint
import time
import MySQLdb
import random
import string
import datetime
from httplib import BadStatusLine
import multiprocessing
from simplejson import loads


##CONSTANTS##
POST_LIMIT = '500'#should be high if relying on dates

#FACEBOOK_ID = '100002642027193'
HOST = 'localhost'
USER = 'root'
DB = 'facebook'
PASSWORD = 'Study@littlebird'
DEF_MESSAGE_TABLE = 'messages'
DEF_PULL_PERCENTAGE = float(0.2)
DEF_PARALLELS = int(5)
# DEF_RANDOM_TERMS = 'the | of | and | to | a | in | is | for | that | with | on'
DEF_RANDOM_TERMS = 'the'
#FRQ_TABLE = 'freqs'

# (Maarten) This is the info from the bogus app that I made using the Penn Perma FB account... The app is called WWBPLab3Sample  
app_id = '653943651304297'
app_secret = '1bc923adced1393d7565dca05a0ea24e'
access_token = urlopen("https://graph.facebook.com/oauth/access_token?client_id="+app_id+"&client_secret="+app_secret+"&grant_type=client_credentials").read()
print access_token

MAX_ERRORS = 10 #how many times to retry something before exiting
ERROR_PAUSE = 7#how many second to pause after an error
PROCESS_CHECK_DELAY = 0.66 #this is multiplied by a function of the limit and parallels to determine how long to delay between spawning a new process
MIN_RESULTS = 2000000

LOGFILE_PREFIX = "randomFB-"
LOGFILE_BASE = "logfile-base"
LOGFILE_SUFFIX = ".log"
LOGFILE_DIR = "/data/log/randomFB/"

##OTHER VARIABLES##
pp = pprint.PrettyPrinter()

def getRandomTerm():
    return random.sample('the of and to'.split(), 1)[0]

def readJSONUrl(url):
    """reads a url aurlopennd interprits the results using JSON"""
    count = 0;
    #print url#debug
    while (True):
        try:
            warn("trying "+url+'\n')#debug
            return loads(urlopen(url).read())
            break
        except (URLError, BadStatusLine), e:
            count += 1
            if (count < MAX_ERRORS): 
                warn("      ERROR: " + str(sys.exc_info()[0]) + ", " + str(e) + ", " + str(count) + " try, trying again in " + str(ERROR_PAUSE) + " seconds")
                warn(e.message)
                # warn("      " + str(type(e)) )
                # warn("      " + str(e.args) )
                # warn("      " + str(e) )
                # time.sleep(ERROR_PAUSE)
                time.sleep(ERROR_PAUSE**count)
            else:
                warn("URL ERROR: too many tries, exiting")
                sys.exit(1);


def loadLexicon(filename):
    """Loads the perma lexicon, using standard formatting"""
    lexFile = open(filename)
    comma = re.compile(r'\,\s*')
    cats = []
    lexicon = {}
    for line in lexFile:
        terms = comma.split(line.rstrip())
        if len(cats) > 0:
            for i in range(len(terms)):
                if (terms[i]):
                    lexicon[cats[i]].append(terms[i])
        else:
            for i in range(len(terms)):
                lexicon[terms[i]] = []
                cats.append(terms[i])
    return (lexicon, cats)

def convertTimeToInt(timestr):
    """converts from format Year-month-day to second from epoch"""
    return int(time.mktime(time.strptime(timestr, '%Y-%m-%dT%H:%M:%S')))

def convertIntToTime(tint):
    """converts from second from epoch to format Year-month-dayThour:min:sec"""
    return str(time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(tint)))

def decrementTime(timestr):
    """decrements the time by 1 second"""
    #warn("dec timestr:"+timestr)#debug
    newTime = convertIntToTime(convertTimeToInt(timestr)-1)
    #warn("dec newTime:"+newTime)#debug
    return newTime
    
def stripPlusTime(timestr):
    return timestr.rstrip('0123456789').rstrip('+')

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

def replaceNonAscii(s): return "".join(map(lambda x: ord(x)<128 and x or '*', s))

def removeHtml(s):
    x = re.compile(r'<[^<]*?/?>')
    return x.sub('', s)

def data2html(data):
    """returns an html respresentation of facebook message data"""
    string = "<html><head><title>facebook data</title></head>\n"
    string += "<body><ul>\n"
    for d in data:
        string += '<li style="margin-bottom:6pt">'
        message = replaceNonAscii(removeHtml(d['message']))
        if 'cat' in d: string+= "<em>"+d['cat']+"</em>: "
        if 'term' in d: 
            term = d['term']
            string += '<span style="color:#004466">'+term+'</span>'
            search = re.compile(r'\b(%s)\b' % term, re.I)
            message = search.sub('<span style="color:#004466;text-decoration:underline">\\1</span>', message)
        string += '<br />' + message + '</li>'+"\n"
    string += "</ul></body></html>\n"
            
    return string


def fbSearchAndInsertTerm(term, urlQuery, options, cat, shared_totalResultCount=None):
    print 'fbSearchAndInsertTerm  starts'
    ##CONNECT TO MYSQL##
    (dbConn, dbCursor) = dbConnect()
    searchterm = term
    if options.is_random_sample==False:
        searchterm = term.replace(" ", "_")
    else:
        searchterm = term.replace(" ", '%20')

    #grep the post or just use message and eliminate if its not in message
    totalResults = 0
    lastResults = 1
    since = options.since
    until = options.until
    if options.is_random_sample:
        # This is whats called
        addParms = '&q='+searchterm
        if (since): addParms += '&since=' + str(since) 
        if (until): addParms += '&until=' + str(until)
        addParms += "&"+access_token
        warn('&since=' + str(since)+ '&until=' + str(until))
        #warn (urlQuery+addParms + "\n")#debug
        results = readJSONUrl(urlQuery+addParms)['data']
        print readJSONUrl(urlQuery+addParms)
        warn("    --[" + str(since) + "] got " + str(len(results)) + " results")
        #pp.pprint(map(lambda x: 'message' in x and searchterm in x['message'] and x['message'] or '<term not in message field>', results['data']))#debug
        #pp.pprint(results)#debug
        insertRandomMessage(dbCursor, [d for d in results if ('message' in d) and validChars(d['message']) ], options.message_table, shared_totalResultCount)        
    # paging
    else:
        while (totalResults < options.limit) and (lastResults > 0):
            addParms = '&q='+searchterm
            if (since): addParms += '&since=' + str(since) 
            if (until): addParms += '&until=' + str(until)
            addParms += "&"+access_token

            warn('&since=' + str(since)+ '&until=' + str(until))
            warn (urlQuery+addParms + "\n")#debug
            results = readJSONUrl(urlQuery+addParms)['data']
            lastResults = len(results)
            totalResults += lastResults
            warn("    --[" + term + "] got " + str(lastResults) + " results (" + str(totalResults) + " total, until=" + str(until) + ")")
            #pp.pprint(map(lambda x: 'message' in x and searchterm in x['message'] and x['message'] or '<term not in message field>', results['data']))#debug
            #pp.pprint(results['data'])
            insertTermMessage(dbCursor, term, [d for d in results if ('message' in d) and (searchterm.lower() in d['message'].lower())], cat)

            if (lastResults):#adjust to check for more posts:
                #find time of last post (should be earliest)
                earliestTime = results[-1]['created_time']
                earliestTime = stripPlusTime(earliestTime)
                until = decrementTime(earliestTime)

        warn ("  [" + term + "] complete.")
    dbConn.close()
    
    #os._exit(0)


def convertTimeToInt(timestr):
    """converts from format Year-month-day to second from epoch"""
    if re.search('\-\d\d$', timestr):
        timestr += 'T00:00:00'
    return int(time.mktime(time.strptime(timestr, '%Y-%m-%dT%H:%M:%S')))

def convertIntToTime(tint):
    """converts from second from epoch to format Year-month-dayThour:min:sec"""
    return str(time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(tint)))

def makeRandomlySortedTimes(since, until):
    """given two unique dates, returns a randomly sorted list of all 'Time'-seconds"""
    """dates are in 'YYYY-MM-DD' format. if same day is used, [] is returned"""
    since_epoch_time = convertTimeToInt(since)
    until_epoch_time = convertTimeToInt(until)
    randomly_sorted_seconds = range(since_epoch_time, until_epoch_time)
    #random.shuffle(randomly_sorted_seconds)
    randomly_sorted_times = map( convertIntToTime, randomly_sorted_seconds )
    return randomly_sorted_times

def getRandTimes(since, until, percent):
#requires math, random, time                                                                            
    since_epoch_time = convertTimeToInt(since)
    until_epoch_time = convertTimeToInt(until)

    post_limit = math.floor((until_epoch_time - since_epoch_time)*percent)

    time_dict = {}
    while len(time_dict) < post_limit:
        rand_epoch_time = random.randint(since_epoch_time, until_epoch_time)
        if time_dict.has_key(rand_epoch_time)==False:
            time_dict[rand_epoch_time] = ( len(time_dict), convertIntToTime(rand_epoch_time) )

    return ( time_dict )

def validChars(msg):
    for x in msg:
        if x not in string.printable: return False
    return True


#####################################################
## DB Methods:

def dbConnect():
    dbConn = MySQLdb.connect (host = HOST,
                          user = USER,
                          passwd = PASSWORD,
                          db = DB)
    dbCursor = dbConn.cursor()
    return (dbConn, dbCursor)

def dbConnectDict():
    dbConn = MySQLdb.connect (host = HOST,
                          user = USER,
                          db = DB)
    dbCursor = dbConn.cursor(MySQLdb.cursors.DictCursor)
    return (dbConn, dbCursor)


def insertTermMessage (dbCursor, term, messageData, cat):
    ##TODO replace with below
    """inserts a row into the mysql database for a given term is message data"""
    sqlQuery = """REPLACE INTO messages (id, term, perma, message, created_time, updated_time, from_id) values (%s, %s, %s, %s, %s, %s, %s)"""
    values = map(lambda d: (d['id'] + '.' + term, term, cat, d['message'].rstrip(), stripPlusTime(d['created_time']), stripPlusTime(d['updated_time']), d['from']['id']), messageData)
    #print "QUERY:" + sqlQuery
    #print "VALUES: " + str(values)
    count = 0;
    while(True):
        try:
            dbCursor.executemany(sqlQuery, values)
            break
        except MySQLdb.Error, e:
            count += 1
            warn("   ["+term+"]MYSQL ERROR" + str(e));
            if (count > MAX_ERRORS): 
                sys.exit(1)
            else:
                time.sleep(ERROR_PAUSE)
                (tmp, dbCursor) = dbConnect()

def insertRandomMessage (dbCursor, messageData, msgTable='msgTable', shared_totalResultCount=None):
    """inserts a row into the mysql database for a given term is message data"""
    warn("Inserting messages into table")
    sqlQuery = """REPLACE INTO """+msgTable+""" (id, message, created_time, updated_time, from_id) values (%s, %s, %s, %s, %s)"""
    values = map(lambda d: (d['id'], d['message'].rstrip(), stripPlusTime(d['created_time']), stripPlusTime(d['updated_time']), d['from']['id']), messageData)
    #print "QUERY:" + sqlQuery #debug
    #print "VALUES: " + str(values) #debug

    warn("      ...inserting " + str(len(values)) + " valid messages")
    count = 0;
    while(True):
        try:
            dbCursor.executemany(sqlQuery, values)
            if shared_totalResultCount:
                shared_totalResultCount.value = shared_totalResultCount.value + len(values)
            break
        except MySQLdb.Error, e:
            count += 1
            warn("  []MYSQL ERROR" + str(e));
            if (count > MAX_ERRORS): 
                sys.exit(1)
            else:
                time.sleep(ERROR_PAUSE)
                (tmp, dbCursor) = dbConnect()


 
def warn(string, log=False, basename = LOGFILE_BASE):
    if log:
        LOGFILE = LOGFILE_DIR + LOGFILE_PREFIX + basename + LOGFILE_SUFFIX
        logfile = file(LOGFILE, 'a')
        logfile.write(string + '\n')
        logfile.close()
    print >>sys.stderr, string


#######################################################
## Main Methods

def facebook2SQL(options):
    print 'facebook2SQL starts'
    """Loads facebook messages with terms from the lexicon to SQL"""
    if not options.filename:
        print "Must provide the lexicon filename as a parameter\n"
        sys.exit()

    ##LOAD LEXICON##
    warn("[LOADING LEXICON]\n")
    if not options.filename:
        print "Must provide the lexicon filename as a parameter\n"
        sys.exit()
    (lexicon, cats) = loadLexicon(options.filename)

    ##RUN QUERIES##
    urlQuery = 'https://graph.facebook.com/search?type=post'
    urlQuery += '&limit=' + str(options.limit)
    if (options.access_token): urlQuery += '&access_token=' + str(options.access_token)
    print 'Run Query'
    warn("URL QUERY: " + urlQuery)
    
    #Parallel setup
    processes = []
    processDelay = int(PROCESS_CHECK_DELAY * int(math.floor(max(0, (math.log(options.limit/min(50, options.limit)) - 0.5)) * math.sqrt(options.parallels))))
    warn("Process delay set to: "+str(processDelay))

    cats.sort()
    cats.reverse()#reversed to start with R, the most freq. cat
    for cat in cats: 
        warn("\n[PUTTING FACEBOOK RESULTS FOR " + cat + " INTO MYSQL]")
        for term in lexicon[cat]:
            #print term, os.environ['FB_FORK']
            while len(processes) >= int(options.parallels):
                #search for stopped processes
                for proc in processes:
                    if not proc.is_alive(): 
                        processes.remove(proc)
                        proc.join()
                time.sleep(processDelay)
            ##fork it out:
            p = multiprocessing.Process(target=fbSearchAndInsertTerm, args=(term, urlQuery, options, cat))
            warn ("  [" + term + "] process starting. (processes already running: " + str(len(processes)) + ")")
            p.start()
            processes.append(p)

    #wait for remaining processes
    for proc in processes:
        processes.remove(proc)
        proc.join()

    warn ("All Processes Complete: "+time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()) )

def datetimeToSinceUntilDay(datetime):
    since = str(datetime.year) + "-" + str(datetime.month) + "-" + str(datetime.day) + "T00:00:00"
    until = str(datetime.year) + "-" + str(datetime.month) + "-" + str(datetime.day) + "T23:59:59"
    return (since, until)

def randomFacebook2SQL(options):
    # (lexicon, cats) = loadLexicon(options.filename)
    # random_terms
    # pull_percentage
    since = ""
    until = ""
    if options.XdaysAgo:
        now = datetime.datetime.now()
        dateXdaysAgo = now - datetime.timedelta( options.XdaysAgo, 0, 0 )
        (since, until) = datetimeToSinceUntilDay( dateXdaysAgo )
    else:
        since = options.since
        until = options.until
        if not since or not until:
            print "Must provide since and until as parameters.\n"
            sys.exit()

    logname = "S_%s---U_%s"%(since, until)
    warn( "randomFacebook2SQL started at: " + str(datetime.datetime.now()) + "  over the period " + logname, True, logname )

    ##RUN QUERIES##
    urlQuery = 'https://graph.facebook.com/search?type=post'
    urlQuery += '&limit=' + str(options.limit)
    if (options.access_token): urlQuery += '&access_token=' + str(options.access_token)
    warn("URL QUERY: " + urlQuery, True, logname)
    
    #Parallel setup
    processes = []
    processDelay = int(PROCESS_CHECK_DELAY * int(math.floor(max(0, (math.log(options.limit/min(50, options.limit)) - 0.5)) * math.sqrt(options.parallels))))
    warn("Process delay set to: "+str(processDelay), True, logname)

    pull_percentage = options.pull_percentage
    random_terms = options.random_terms
    randomly_sorted_time_list = makeRandomlySortedTimes(since, until)
    print since, until
    total_seconds = len(randomly_sorted_time_list)

    shared_totalResultCount = multiprocessing.Value('d', 0)
    warn("\n[PUTTING RANDOM FACEBOOK RESULTS FOR " + str(pull_percentage) + " PERCENT OF (" + since + ", " + until + ") INTO MYSQL]", True, logname)
    
    while ( shared_totalResultCount.value < MIN_RESULTS ) and ( (total_seconds - len(randomly_sorted_time_list)) < pull_percentage * total_seconds ):
    #*** while (shared_lockable_variable < MIN_RESULTS) && (len(rand_sorted_time_list) > 0.50*total_seconds_in_day):
        ##*** epoch_time = rand_sorted_time_list.pop()

        random_terms = getRandomTerm()
        random_time = randomly_sorted_time_list.pop()
        ##print term, os.environ['FB_FORK']
        while len(processes) >= int(options.parallels):
            #search for stopped processes
            for proc in processes:
                if not proc.is_alive(): 
                    processes.remove(proc)
                    warn("  ["+str(proc.name)+"] removed.")
                    proc.join()
            warn("ACTIVE PROCS: %s" % str(map(lambda p: p.name, processes)))
            warn("%d total results attained so far" % shared_totalResultCount.value )
            warn("%5.3f percent complete" % (shared_totalResultCount.value * 1.0 / MIN_RESULTS))
            time.sleep(processDelay)

        ##fork it out:
        #options.since = random_time
        #options.until = random_time
        options.since = convertIntToTime(convertTimeToInt(random_time) - 1)
        options.until = convertIntToTime(convertTimeToInt(random_time) + 1)

        #*** might need to pass a shared lockable variable
        p = multiprocessing.Process(target=fbSearchAndInsertTerm, args=(random_terms, urlQuery, options, "NULL", shared_totalResultCount))
        warn ("  [" + str(p.name) + ": "+ options.since +"] starting. (procs already running: " + str(len(processes)) + ")")
        p.start()
        processes.append(p)

    #wait for remaining processes
    for proc in processes:
        processes.remove(proc)
        proc.join()

    warn ("All Processes Complete: "+time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()), True, logname)
    warn ( "Total messages added count: %d"%shared_totalResultCount.value, True, logname)



def random2html(options):
    """Outputs html examples of terms"""

    ##SETUP WHERE##
    wheres = []
    (lexicon,cats) = (None, None)
    if options.filename:
        (lexicon, cats) = loadLexicon(options.filename)
        terms = []
        for cat in cats: 
            for term in lexicon[cat]:
                terms.append(term)
        wheres.append(" term IN ('" + "','".join(terms) + "') ")
    if options.where: wheres.append(options.where)

    ##SETUP QUERY##
    sqlQuery = "SELECT term, message FROM messages "
    if wheres: sqlQuery += "WHERE " + " AND ".join(wheres)
    sqlQuery += " ORDER BY RAND() "
    sqlQuery += " LIMIT " + options.num
    warn ("Query: " + sqlQuery)
    
    
    ##CONNECT TO MYSQL##
    (dbConn, dCursor) = dbConnectDict()


    ##EXECUTE QUERY##
    data = []
    count = 0
    while(True):
        try:
            dCursor.execute(sqlQuery)
            data = dCursor.fetchall()
            break
        except MySQLdb.Error, e:
            count += 1
            if (count > MAX_ERRORS): 
                warn(" Too Many errors, MYSQL ERROR" + str(e))
                sys.exit(1)
            else:
                time.sleep(ERROR_PAUSE)

    ##ADD CATS##
    if cats:
        for cat in cats:
            for d in data:
                if d['term'] in lexicon[cat]:
                    d['cat'] = cat;
                    
    ##OUTPUT##
    print data2html(data)




#########################################################
## MAIN AREA:
if __name__=="__main__":

    ##SETUP ARGUMENTS##
    optParser = OptionParser()
    #For facebook2SQL:
    optParser.add_option("-f", "--file", dest="filename",
                         help="Lexicon Filename")
    optParser.add_option("-s", "--since", dest="since",
                         help="Since (start) date for search, format is xxxx-xx-xxTxx:xx:xx (year, month, day, hour, minute, second)")
    optParser.add_option("-u", "--until", dest="until",
                         help="Until (end) date for search, format is  xxxx-xx-xxTxx:xx:xx (year, month, day, hour, minute, second)")
    optParser.add_option("-a", "--access_token", dest="access_token",
                         help="Specify an access token given from facebook (be sure you are not using your personal account)")
    optParser.add_option("-l", "--limit", dest="limit", type=int, default=int(POST_LIMIT),
                         help="limit the number of posts to return")
    optParser.add_option("-p", "--parallels", dest="parallels", type=int, default=DEF_PARALLELS,
                         help="number of parallel processes to fork off to run fb queries simultaneously")
    optParser.add_option("-m", "--message", dest="message_table", default=DEF_MESSAGE_TABLE,
                         help="name of database message table to use for message insertion")

    #For randomFacebook2SQL (luke additions):
    optParser.add_option("-x", "--is_random_sample", action="store_true", dest="is_random_sample", default=False,
                         help="If True, ensures the fb status scrape is random")
    optParser.add_option("-c", "--pull_percentage", dest="pull_percentage", type=float, default=DEF_PULL_PERCENTAGE,
                         help="sets the percent of the random scrape to include from the time covered between --since and --until. 0.9 indicates we would accept random statuses from since 90% of the way through until.")
    optParser.add_option("--random_terms", dest="random_terms", type=str, default=DEF_RANDOM_TERMS,
                         help="append a string of terms to use to simulate a random sample. Need a | delimiter. Ex: 'one | on | with | at | in' ")
    optParser.add_option("-X", "--XdaysAgo", dest="XdaysAgo", type=int,                                    
                         help="If enabled, pulls the random sample from \XdaysAgo\" days earlier")
    optParser.add_option("-M", dest="minresults", type=int, default=2000000,
                         help="How many posts to pull at a minimum")
    optParser.add_option("-D", "--pull_diff", action="store_true", dest="pulldiff", default=False,
                         help="If enabled, automatically pulls the difference between MIN_RESULTS and current count")


    #For random2html:
    optParser.add_option("-r", "--random", action="store_true", dest="random",
                         help="Pull out a random sample of posts from sql (must provide --num)")
    optParser.add_option("-n", "--num", dest="num", default=int(10),
                         help="Number of posts to output")
    optParser.add_option("-w", "--where", dest="where",
                         help="where phrase to add to sql query")
    (options,args) = optParser.parse_args()

    ##DETERMINE WHAT FUNCTION TO RUN##
    if options.random:
        random2html(options)
    elif options.is_random_sample:
        if options.minresults:
            MIN_RESULTS = options.minresults
        randomFacebook2SQL(options)
    else:#default mode, import sql
        facebook2SQL(options)

