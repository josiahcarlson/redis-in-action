
import bisect
import contextlib
import csv
from datetime import datetime
import functools
import json
import logging
import random
import threading
import time
import unittest
import uuid

import redis

QUIT = False
SAMPLE_COUNT = 100

config_connection = None

# <start id="recent_log"/>
SEVERITY = {                                                    #A
    logging.DEBUG: 'debug',                                     #A
    logging.INFO: 'info',                                       #A
    logging.WARNING: 'warning',                                 #A
    logging.ERROR: 'error',                                     #A
    logging.CRITICAL: 'critical',                               #A
}                                                               #A
SEVERITY.update((name, name) for name in SEVERITY.values())     #A

def log_recent(conn, name, message, severity=logging.INFO, pipe=None):
    severity = str(SEVERITY.get(severity, severity)).lower()    #B
    destination = 'recent:%s:%s'%(name, severity)               #C
    message = time.asctime() + ' ' + message                    #D
    pipe = pipe or conn.pipeline()                              #E
    pipe.lpush(destination, message)                            #F
    pipe.ltrim(destination, 0, 99)                              #G
    pipe.execute()                                              #H
# <end id="recent_log"/>
#A Set up a mapping that should help turn most logging severity levels into something consistent
#B Actually try to turn a logging level into a simple string
#C Create the key that messages will be written to
#D Add the current time so that we know when the message was sent
#E Set up a pipeline so we only need 1 round trip
#F Add the message to the beginning of the log list
#G Trim the log list to only include the most recent 100 messages
#H Execute the two commands
#END

# <start id="common_log"/>
def log_common(conn, name, message, severity=logging.INFO, timeout=5):
    severity = str(SEVERITY.get(severity, severity)).lower()    #A
    destination = 'common:%s:%s'%(name, severity)               #B
    start_key = destination + ':start'                          #C
    pipe = conn.pipeline()
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)                               #D
            now = datetime.utcnow().timetuple()                 #E
            hour_start = datetime(*now[:4]).isoformat()         #F

            existing = pipe.get(start_key)
            pipe.multi()                                        #H
            if existing and existing < hour_start:              #G
                pipe.rename(destination, destination + ':last') #I
                pipe.rename(start_key, destination + ':pstart') #I
                pipe.set(start_key, hour_start)                 #J
            elif not existing:                                  #J
                pipe.set(start_key, hour_start)                 #J

            pipe.zincrby(destination, message)                  #K
            log_recent(pipe, name, message, severity, pipe)     #L
            return
        except redis.exceptions.WatchError:
            continue                                            #M
# <end id="common_log"/>
#A Handle the logging level
#B Set up the destination key for keeping recent logs
#C Keep a record of the start of the hour for this set of messages
#D We are going to watch the start of the hour key for changes that only happen at the beginning of the hour
#E Get the current time
#F Find the current start hour
#G If the current list of common logs is for a previous hour
#H Set up the transaction
#I Move the old common log information to the archive
#J Update the start of the current hour for the common logs
#K Actually increment our common counter
#L Call the log_recent() function to record these there, and rely on its call to execute()
#M If we got a watch error from someone else archiving, try again
#END

# <start id="update_counter"/>
PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]         #A

def update_counter(conn, name, count=1, now=None):
    now = now or time.time()                            #B
    pipe = conn.pipeline()                              #C
    for prec in PRECISION:                              #D
        pnow = int(now / prec) * prec                   #E
        hash = '%s:%s'%(prec, name)                     #F
        pipe.zadd('known:', hash, 0)                    #G
        pipe.hincrby('count:' + hash, pnow, count)      #H
    pipe.execute()
# <end id="update_counter"/>
#A The precision of the counters in seconds: 1 second, 5 seconds, 1 minute, 5 minutes, 1 hour, 5 hours, 1 day - adjust as necessary
#B Get the current time to know when is the proper time to add to
#C Create a transactional pipeline so that later cleanup can work correctly
#D Add entries for all precisions that we record
#E Get the start of the current time slice
#F Create the named hash where this data will be stored
#G Record a reference to the counters into a ZSET with the score 0 so we can clean up after ourselves
#H Update the counter for the given name and time precision
#END

# <start id="get_counter"/>
def get_counter(conn, name, precision):
    hash = '%s:%s'%(precision, name)                #A
    data = conn.hgetall('count:' + hash)            #B
    to_return = []                                  #C
    for key, value in data.iteritems():             #C
        to_return.append((int(key), int(value)))    #C
    to_return.sort()                                #D
    return to_return
# <end id="get_counter"/>
#A Get the name of the key where we will be storing counter data
#B Fetch the counter data from Redis
#C Convert the counter data into something more expected
#D Sort our data so that older samples are first
#END

# <start id="clean_counters"/>
def clean_counters(conn):
    pipe = conn.pipeline(True)
    passes = 0                                                  #A
    while not QUIT:                                             #C
        start = time.time()                                     #D
        index = 0                                               #E
        while index < conn.zcard('known:'):                     #E
            hash = conn.zrange('known:', index, index)          #F
            index += 1
            if not hash:
                break
            hash = hash[0]
            prec = int(hash.partition(':')[0])                  #G
            bprec = int(prec // 60) or 1                        #H
            if passes % bprec:                                  #I
                continue

            hkey = 'count:' + hash
            cutoff = time.time() - SAMPLE_COUNT * prec          #J
            samples = map(int, conn.hkeys(hkey))                #K
            samples.sort()                                      #L
            remove = bisect.bisect_right(samples, cutoff)       #L

            if remove:                                          #M
                conn.hdel(hkey, *samples[:remove])              #M
                if remove == len(samples):                      #N
                    try:
                        pipe.watch(hkey)                        #O
                        if not pipe.hlen(hkey):                 #P
                            pipe.multi()                        #P
                            pipe.zrem('known:', hash)           #P
                            pipe.execute()                      #P
                            index -= 1                          #B
                        else:
                            pipe.unwatch()                      #Q
                    except redis.exceptions.WatchError:         #R
                        pass                                    #R

        passes += 1                                             #S
        duration = min(int(time.time() - start) + 1, 60)        #S
        time.sleep(max(60 - duration, 1))                       #T
# <end id="clean_counters"/>
#A Keep a record of the number of passes so that we can balance cleaning out per-second vs. per-day counters
#C Keep cleaning out counters until we are told to stop
#D Get the start time of the pass to calculate the total duration
#E Incrementally iterate over all known counters
#F Get the next counter to check
#G Get the precision of the counter
#H We are going to be taking a pass every 60 seconds or so, so we are going to try to clean out counters at roughly the rate that they are written to
#I Try the next counter if we aren't supposed to check this one on this pass (for example, we have taken 3 passes, but the counter has a precision of 5 minutes)
#J Find the cutoff time for the earliest sample that we should keep, given the precision and number of samples that we want to keep
#K Fetch the times of the samples, and convert the strings to integers
#L Determine the number of samples that should be deleted
#M Remove the samples as necessary
#N We have a reason to potentially remove the counter from the list of known counters ZSET
#O Watch the counter hash for changes
#P Verify that the counter hash is empty, and if so, remove it from the known counters
#B If we deleted a counter, then we can use the same index next pass
#Q The hash is not empty, keep it in the list of known counters
#R Someone else changed the counter hash by adding counters, which means that it has data, so we will leave the counter in the list of known counters
#S Update our passes and duration variables for the next pass, as an attempt to clean out counters as often as they are seeing updates
#T Sleep the remainder of the 60 seconds, or at least 1 second, just to offer a bit of a rest
#END

# <start id="update_stats"/>
def update_stats(conn, context, type, value, timeout=5):
    destination = 'stats:%s:%s'%(context, type)                 #A
    start_key = destination + ':start'                          #B
    pipe = conn.pipeline(True)
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)                               #B
            now = datetime.utcnow().timetuple()                 #B
            hour_start = datetime(*now[:4]).isoformat()         #B

            existing = pipe.get(start_key)
            pipe.multi()
            if existing and existing < hour_start:
                pipe.rename(destination, destination + ':last') #B
                pipe.rename(start_key, destination + ':pstart') #B
                pipe.set(start_key, hour_start)                 #B

            tkey1 = str(uuid.uuid4())
            tkey2 = str(uuid.uuid4())
            pipe.zadd(tkey1, 'min', value)                      #C
            pipe.zadd(tkey2, 'max', value)                      #C
            pipe.zunionstore(destination,                       #D
                [destination, tkey1], aggregate='min')          #D
            pipe.zunionstore(destination,                       #D
                [destination, tkey2], aggregate='max')          #D

            pipe.delete(tkey1, tkey2)                           #E
            pipe.zincrby(destination, 'count')                  #F
            pipe.zincrby(destination, 'sum', value)             #F
            pipe.zincrby(destination, 'sumsq', value*value)     #F

            return pipe.execute()[-3:]                          #G
        except redis.exceptions.WatchError:
            continue                                            #H
# <end id="update_stats"/>
#A Set up the destination statistics key
#B Handle the current hour/last hour like in common_log()
#C Add the value to the temporary keys
#D Union the temporary keys with the destination stats key with the appropriate min/max aggregate
#E Clean up the temporary keys
#F Update the count, sum, and sum of squares members of the zset
#G Return the base counter info so that the caller can do something interesting if necessary
#H If the hour just turned over and the stats have already been shuffled over, try again
#END

# <start id="get_stats"/>
def get_stats(conn, context, type):
    key = 'stats:%s:%s'%(context, type)                                 #A
    data = dict(conn.zrange(key, 0, -1, withscores=True))               #B
    data['average'] = data['sum'] / data['count']                       #C
    numerator = data['sumsq'] - data['sum'] ** 2 / data['count']        #D
    data['stddev'] = (numerator / (data['count'] - 1 or 1)) ** .5       #E
    return data
# <end id="get_stats"/>
#A Set up the key that we are fetching our statistics from
#B Fetch our basic statistics and package them as a dictionary
#C Calculate the average
#D Prepare the first part of the calculation of standard deviation
#E Finish our calculation of standard deviation
#END


# <start id="access_time_context_manager"/>
@contextlib.contextmanager                                              #A
def access_time(conn, context):
    start = time.time()                                                 #B
    yield                                                               #C

    delta = time.time() - start                                         #D
    stats = update_stats(conn, context, 'AccessTime', delta)            #E
    average = stats[1] / stats[0]                                       #F

    pipe = conn.pipeline(True)
    pipe.zadd('slowest:AccessTime', context, average)                   #G
    pipe.zremrangebyrank('slowest:AccessTime', 0, -101)                 #H
    pipe.execute()
# <end id="access_time_context_manager"/>
#A Make this Python generator into a context manager
#B Record the start time
#C Let the block of code that we are wrapping run
#D Calculate the time that the block took to execute
#E Update the stats for this context
#F Calculate the average
#G Add the average to a ZSET that holds the slowest access times
#H Keep the slowest 100 items in the AccessTime ZSET
#END

# <start id="access_time_use"/>
def process_view(conn, callback):               #A
    with access_time(conn, request.path):       #B
        return callback()                       #C
# <end id="access_time_use"/>
#A This example web view takes the Redis connection as well as a callback to generate the content
#B This is how you would use the access time context manager to wrap a block of code
#C This is executed when the 'yield' statement is hit from within the context manager
#END

# <start id="_1314_14473_9188"/>
def ip_to_score(ip_address):
    score = 0
    for v in ip_address.split('.'):
        score = score * 256 + int(v, 10)
    return score
# <end id="_1314_14473_9188"/>
#END

# <start id="_1314_14473_9191"/>
def import_ips_to_redis(conn, filename):                #A
    csv_file = csv.reader(open(filename, 'rb'))
    for count, row in enumerate(csv_file):
        start_ip = row[0] if row else ''                #B
        if 'i' in start_ip.lower():
            continue
        if '.' in start_ip:                             #B
            start_ip = ip_to_score(start_ip)            #B
        elif start_ip.isdigit():                        #B
            start_ip = int(start_ip, 10)                #B
        else:
            continue                                    #C

        city_id = row[2] + '_' + str(count)             #D
        conn.zadd('ip2cityid:', city_id, start_ip)      #E
# <end id="_1314_14473_9191"/>
#A Should be run with the location of the GeoLiteCity-Blocks.csv file
#B Convert the IP address to a score as necessary
#C Header row or malformed entry
#D Construct the unique city id
#E Add the IP address score and City ID
#END

# <start id="_1314_14473_9194"/>
def import_cities_to_redis(conn, filename):         #A
    for row in csv.reader(open(filename, 'rb')):
        if len(row) < 4 or not row[0].isdigit():
            continue
        row = [i.decode('latin-1') for i in row]
        city_id = row[0]                            #B
        country = row[1]                            #B
        region = row[2]                             #B
        city = row[3]                               #B
        conn.hset('cityid2city:', city_id,          #C
            json.dumps([city, region, country]))    #C
# <end id="_1314_14473_9194"/>
#A Should be run with the location of the GeoLiteCity-Location.csv file
#B Prepare the information for adding to the hash
#C Actually add the city information to Redis
#END

# <start id="_1314_14473_9197"/>
def find_city_by_ip(conn, ip_address):
    if isinstance(ip_address, str):                        #A
        ip_address = ip_to_score(ip_address)               #A

    city_id = conn.zrevrangebyscore(                       #B
        'ip2cityid:', ip_address, 0, start=0, num=1)       #B

    if not city_id:
        return None

    city_id = city_id[0].partition('_')[0]                 #C
    return json.loads(conn.hget('cityid2city:', city_id))  #D
# <end id="_1314_14473_9197"/>
#A Convert the IP address to a score for zrevrangebyscore
#B Find the uique city ID
#C Convert the unique city ID to the common city ID
#D Fetch the city information from the hash
#END

# <start id="is_under_maintenance"/>
LAST_CHECKED = None
IS_UNDER_MAINTENANCE = False

def is_under_maintenance(conn):
    global LAST_CHECKED, IS_UNDER_MAINTENANCE   #A

    if LAST_CHECKED < time.time() - 1:          #B
        LAST_CHECKED = time.time()              #C
        IS_UNDER_MAINTENANCE = bool(            #D
            conn.get('is-under-maintenance'))   #D

    return IS_UNDER_MAINTENANCE                 #E
# <end id="is_under_maintenance"/>
#A Set the two variables as globals so we can write to them later
#B Check to see if it has been at least 1 second since we last checked
#C Update the last checked time
#D Find out whether the system is under maintenance
#E Return whether the system is under maintenance
#END

# <start id="set_config"/>
def set_config(conn, type, component, config):
    conn.set(
        'config:%s:%s'%(type, component),
        json.dumps(config))
# <end id="set_config"/>
#END

# <start id="get_config"/>
CONFIGS = {}
CHECKED = {}

def get_config(conn, type, component, wait=1):
    key = 'config:%s:%s'%(type, component)

    if CHECKED.get(key) < time.time() - wait:           #A
        CHECKED[key] = time.time()                      #B
        config = json.loads(conn.get(key) or '{}')      #C
        config = dict((str(k), config[k]) for k in config)#G
        old_config = CONFIGS.get(key)                   #D

        if config != old_config:                        #E
            CONFIGS[key] = config                       #F

    return CONFIGS.get(key)
# <end id="get_config"/>
#A Check to see if we should update the configuration information about this component
#B We can, so update the last time we checked this connection
#C Fetch the configuration for this component
#G Convert potentially unicode keyword arguments into string keyword arguments
#D Get the old configuration for this component
#E If the configurations are different
#F Update the configuration
#END

# <start id="redis_connection"/>
REDIS_CONNECTIONS = {}

def redis_connection(component, wait=1):                        #A
    key = 'config:redis:' + component                           #B
    def wrapper(function):                                      #C
        @functools.wraps(function)                              #D
        def call(*args, **kwargs):                              #E
            old_config = CONFIGS.get(key, object())             #F
            _config = get_config(                               #G
                config_connection, 'redis', component, wait)    #G

            config = {}
            for k, v in _config.iteritems():                    #L
                config[k.encode('utf-8')] = v                   #L

            if config != old_config:                            #H
                REDIS_CONNECTIONS[key] = redis.Redis(**config)  #H

            return function(                                    #I
                REDIS_CONNECTIONS.get(key), *args, **kwargs)    #I
        return call                                             #J
    return wrapper                                              #K
# <end id="redis_connection"/>
#A We pass the name of the application component to the decorator
#B We cache the configuration key because we will be fetching it every time the function is called
#C Our wrapper takes a function that it wraps with another function
#D Copy some useful metadata from the original function to the configuration handler
#E Create the actual function that will be managing connection information
#F Fetch the old configuration, if any
#G Get the new configuration, if any
#L Make the configuration usable for creating a Redis connection
#H If the new and old configuration do not match, create a new connection
#I Call and return the result of our wrapped function, remembering to pass the connection and the other matched arguments
#J Return the fully wrapped function
#K Return a function that can wrap our Redis function
#END

'''
# <start id="recent_log_decorator"/>
@redis_connection('logs')                   #A
def log_recent(conn, app, message):         #B
    'the old log_recent() code'

log_recent('main', 'User 235 logged in')    #C
# <end id="recent_log_decorator"/>
#A The redis_connection() decorator is very easy to use
#B The function definition doesn't change
#C You no longer need to worry about passing the log server connection when calling log_recent()
#END
'''

#--------------- Below this line are helpers to test the code ----------------

class request:
    pass

# a faster version with pipelines for actual testing
def import_ips_to_redis(conn, filename):
    csv_file = csv.reader(open(filename, 'rb'))
    pipe = conn.pipeline(False)
    for count, row in enumerate(csv_file):
        start_ip = row[0] if row else ''
        if 'i' in start_ip.lower():
            continue
        if '.' in start_ip:
            start_ip = ip_to_score(start_ip)
        elif start_ip.isdigit():
            start_ip = int(start_ip, 10)
        else:
            continue

        city_id = row[2] + '_' + str(count)
        pipe.zadd('ip2cityid:', city_id, start_ip)
        if not (count+1) % 1000:
            pipe.execute()
    pipe.execute()

def import_cities_to_redis(conn, filename):
    pipe = conn.pipeline(False)
    for count, row in enumerate(csv.reader(open(filename, 'rb'))):
        if len(row) < 4 or not row[0].isdigit():
            continue
        row = [i.decode('latin-1') for i in row]
        city_id = row[0]
        country = row[1]
        region = row[2]
        city = row[3]
        pipe.hset('cityid2city:', city_id,
            json.dumps([city, region, country]))
        if not (count+1) % 1000:
            pipe.execute()
    pipe.execute()

class TestCh05(unittest.TestCase):
    def setUp(self):
        global config_connection
        import redis
        self.conn = config_connection = redis.Redis(db=15)
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()
        del self.conn
        global config_connection, QUIT, SAMPLE_COUNT
        config_connection = None
        QUIT = False
        SAMPLE_COUNT = 100
        print
        print

    def test_log_recent(self):
        import pprint
        conn = self.conn

        print "Let's write a few logs to the recent log"
        for msg in xrange(5):
            log_recent(conn, 'test', 'this is message %s'%msg)
        recent = conn.lrange('recent:test:info', 0, -1)
        print "The current recent message log has this many messages:", len(recent)
        print "Those messages include:"
        pprint.pprint(recent[:10])
        self.assertTrue(len(recent) >= 5)

    def test_log_common(self):
        import pprint
        conn = self.conn

        print "Let's write some items to the common log"
        for count in xrange(1, 6):
            for i in xrange(count):
                log_common(conn, 'test', "message-%s"%count)
        common = conn.zrevrange('common:test:info', 0, -1, withscores=True)
        print "The current number of common messages is:", len(common)
        print "Those common messages are:"
        pprint.pprint(common)
        self.assertTrue(len(common) >= 5)

    def test_counters(self):
        import pprint
        global QUIT, SAMPLE_COUNT
        conn = self.conn

        print "Let's update some counters for now and a little in the future"
        now = time.time()
        for delta in xrange(10):
            update_counter(conn, 'test', count=random.randrange(1,5), now=now+delta)
        counter = get_counter(conn, 'test', 1)
        print "We have some per-second counters:", len(counter)
        self.assertTrue(len(counter) >= 10)
        counter = get_counter(conn, 'test', 5)
        print "We have some per-5-second counters:", len(counter)
        print "These counters include:"
        pprint.pprint(counter[:10])
        self.assertTrue(len(counter) >= 2)
        print

        tt = time.time
        def new_tt():
            return tt() + 2*86400
        time.time = new_tt

        print "Let's clean out some counters by setting our sample count to 0"
        SAMPLE_COUNT = 0
        t = threading.Thread(target=clean_counters, args=(conn,))
        t.setDaemon(1) # to make sure it dies if we ctrl+C quit
        t.start()
        time.sleep(1)
        QUIT = True
        time.time = tt
        counter = get_counter(conn, 'test', 86400)
        print "Did we clean out all of the counters?", not counter
        self.assertFalse(counter)

    def test_stats(self):
        import pprint
        conn = self.conn

        print "Let's add some data for our statistics!"
        for i in xrange(5):
            r = update_stats(conn, 'temp', 'example', random.randrange(5, 15))
        print "We have some aggregate statistics:", r
        rr = get_stats(conn, 'temp', 'example')
        print "Which we can also fetch manually:"
        pprint.pprint(rr)
        self.assertTrue(rr['count'] >= 5)

    def test_access_time(self):
        import pprint
        conn = self.conn

        print "Let's calculate some access times..."
        for i in xrange(10):
            with access_time(conn, "req-%s"%i):
                time.sleep(.5 + random.random())
        print "The slowest access times are:"
        atimes = conn.zrevrange('slowest:AccessTime', 0, -1, withscores=True)
        pprint.pprint(atimes[:10])
        self.assertTrue(len(atimes) >= 10)
        print

        def cb():
            time.sleep(1 + random.random())

        print "Let's use the callback version..."
        for i in xrange(5):
            request.path = 'cbreq-%s'%i
            process_view(conn, cb)
        print "The slowest access times are:"
        atimes = conn.zrevrange('slowest:AccessTime', 0, -1, withscores=True)
        pprint.pprint(atimes[:10])
        self.assertTrue(len(atimes) >= 10)

    def test_ip_lookup(self):
        conn = self.conn

        try:
            open('GeoLiteCity-Blocks.csv', 'rb')
            open('GeoLiteCity-Location.csv', 'rb')
        except:
            print "********"
            print "You do not have the GeoLiteCity database available, aborting test"
            print "Please have the following two files in the current path:"
            print "GeoLiteCity-Blocks.csv"
            print "GeoLiteCity-Location.csv"
            print "********"
            return

        print "Importing IP addresses to Redis... (this may take a while)"
        import_ips_to_redis(conn, 'GeoLiteCity-Blocks.csv')
        ranges = conn.zcard('ip2cityid:')
        print "Loaded ranges into Redis:", ranges
        self.assertTrue(ranges > 1000)
        print

        print "Importing Location lookups to Redis... (this may take a while)"
        import_cities_to_redis(conn, 'GeoLiteCity-Location.csv')
        cities = conn.hlen('cityid2city:')
        print "Loaded city lookups into Redis:", cities
        self.assertTrue(cities > 1000)
        print

        print "Let's lookup some locations!"
        rr = random.randrange
        for i in xrange(5):
            print find_city_by_ip(conn, '%s.%s.%s.%s'%(rr(1,255), rr(256), rr(256), rr(256)))

    def test_is_under_maintenance(self):
        print "Are we under maintenance (we shouldn't be)?", is_under_maintenance(self.conn)
        self.conn.set('is-under-maintenance', 'yes')
        print "We cached this, so it should be the same:", is_under_maintenance(self.conn)
        time.sleep(1)
        print "But after a sleep, it should change:", is_under_maintenance(self.conn)
        print "Cleaning up..."
        self.conn.delete('is-under-maintenance')
        time.sleep(1)
        print "Should be False again:", is_under_maintenance(self.conn)

    def test_config(self):
        print "Let's set a config and then get a connection from that config..."
        set_config(self.conn, 'redis', 'test', {'db':15})
        @redis_connection('test')
        def test(conn2):
            return bool(conn2.info())
        print "We can run commands from the configured connection:", test()

if __name__ == '__main__':
    unittest.main()
