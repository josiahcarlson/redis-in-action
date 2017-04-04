
import binascii
import bisect
from datetime import date, timedelta
from collections import defaultdict
import math
import time
import unittest
import uuid

import redis

def readblocks(conn, key, blocksize=2**17):
    lb = blocksize
    pos = 0
    while lb == blocksize:                                  #A
        block = conn.substr(key, pos, pos + blocksize - 1)  #B
        yield block                                         #C
        lb = len(block)                                     #C
        pos += lb                                           #C
    yield ''

'''
# <start id="ziplist-configuration-options"/>
list-max-ziplist-entries 512    #A
list-max-ziplist-value 64       #A

hash-max-ziplist-entries 512    #B
hash-max-ziplist-value 64       #B

zset-max-ziplist-entries 128    #C
zset-max-ziplist-value 64       #C
# <end id="ziplist-configuration-options"/>
#A Limits for ziplist use with LISTs
#B Limits for ziplist use with HASHes (previous versions of Redis used a different name and encoding for this)
#C Limits for ziplist use with ZSETs
#END
'''

'''
# <start id="ziplist-test"/>
>>> conn.rpush('test', 'a', 'b', 'c', 'd')  #A
4                                           #A
>>> conn.debug_object('test')                                       #B
{'encoding': 'ziplist', 'refcount': 1, 'lru_seconds_idle': 20,      #C
'lru': 274841, 'at': '0xb6c9f120', 'serializedlength': 24,          #C
'type': 'Value'}                                                    #C
>>> conn.rpush('test', 'e', 'f', 'g', 'h')  #D
8                                           #D
>>> conn.debug_object('test')
{'encoding': 'ziplist', 'refcount': 1, 'lru_seconds_idle': 0,   #E
'lru': 274846, 'at': '0xb6c9f120', 'serializedlength': 36,      #E
'type': 'Value'}
>>> conn.rpush('test', 65*'a')          #F
9
>>> conn.debug_object('test')
{'encoding': 'linkedlist', 'refcount': 1, 'lru_seconds_idle': 10,   #F
'lru': 274851, 'at': '0xb6c9f120', 'serializedlength': 30,          #G
'type': 'Value'}
>>> conn.rpop('test')                                               #H
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
>>> conn.debug_object('test')
{'encoding': 'linkedlist', 'refcount': 1, 'lru_seconds_idle': 0,    #H
'lru': 274853, 'at': '0xb6c9f120', 'serializedlength': 17,
'type': 'Value'}
# <end id="ziplist-test"/>
#A Let's start by pushing 4 items onto a LIST
#B We can discover information about a particular object with the 'debug object' command
#C The information we are looking for is the 'encoding' information, which tells us that this is a ziplist, which is using 24 bytes of memory
#D Let's push 4 more items onto the LIST
#E We still have a ziplist, and its size grew to 36 bytes (which is exactly 2 bytes overhead, 1 byte data, for each of the 4 items we just pushed)
#F When we push an item bigger than what was allowed for the encoding, the LIST gets converted from the ziplist encoding to a standard linked list
#G While the serialized length went down, for non-ziplist encodings (except for the special encoding for SETs), this number doesn't represent the amount of actual memory used by the structure
#H After a ziplist is converted to a regular structure, it doesn't get re-encoded as a ziplist if the structure later meets the criteria
#END
'''

'''
# <start id="intset-configuration-option"/>
set-max-intset-entries 512      #A
# <end id="intset-configuration-option"/>
#A Limits for intset use with SETs
#END
'''

'''
# <start id="intset-test"/>
>>> conn.sadd('set-object', *range(500))    #A
500
>>> conn.debug_object('set-object')         #A
{'encoding': 'intset', 'refcount': 1, 'lru_seconds_idle': 0,    #A
'lru': 283116, 'at': '0xb6d1a1c0', 'serializedlength': 1010,
'type': 'Value'}
>>> conn.sadd('set-object', *range(500, 1000))  #B
500
>>> conn.debug_object('set-object')             #B
{'encoding': 'hashtable', 'refcount': 1, 'lru_seconds_idle': 0, #B
'lru': 283118, 'at': '0xb6d1a1c0', 'serializedlength': 2874,
'type': 'Value'}
# <end id="intset-test"/>
#A Let's add 500 items to the set and see that it is still encoded as an intset
#B But when we push it over our configured 512 item limit, the intset is translated into a hash table representation
#END
'''

# <start id="rpoplpush-benchmark"/>
def long_ziplist_performance(conn, key, length, passes, psize): #A
    conn.delete(key)                    #B
    conn.rpush(key, *range(length))     #C
    pipeline = conn.pipeline(False)     #D

    t = time.time()                     #E
    for p in xrange(passes):            #F
        for pi in xrange(psize):        #G
            pipeline.rpoplpush(key, key)#H
        pipeline.execute()              #I

    return (passes * psize) / (time.time() - t or .001) #J
# <end id="rpoplpush-benchmark"/>
#A We are going to parameterize everything so that we can measure performance in a variety of ways
#B Start by deleting the named key to ensure that we only benchmark exactly what we intend to
#C Initialize the LIST by pushing our desired count of numbers onto the right end
#D Prepare a pipeline so that we are less affected by network round-trip times
#E Start the timer
#F We will perform a number of pipeline executions provided by 'passes'
#G Each pipeline execution will include 'psize' actual calls to RPOPLPUSH
#H Each call will result in popping the rightmost item from the LIST, pushing to the left end of the same LIST
#I Execute the 'psize' calls to RPOPLPUSH
#J Calculate the number of calls per second that are performed
#END

'''
# <start id="rpoplpush-performance"/>
>>> long_ziplist_performance(conn, 'list', 1, 1000, 100)        #A
52093.558416505381                                              #A
>>> long_ziplist_performance(conn, 'list', 100, 1000, 100)      #A
51501.154762768667                                              #A
>>> long_ziplist_performance(conn, 'list', 1000, 1000, 100)     #A
49732.490843316067                                              #A
>>> long_ziplist_performance(conn, 'list', 5000, 1000, 100)     #B
43424.056529592635                                              #B
>>> long_ziplist_performance(conn, 'list', 10000, 1000, 100)    #B
36727.062573334966                                              #B
>>> long_ziplist_performance(conn, 'list', 50000, 1000, 100)    #C
16695.140684975777                                              #C
>>> long_ziplist_performance(conn, 'list', 100000, 500, 100)    #D
553.10821080054586                                              #D
# <end id="rpoplpush-performance"/>
#A With lists encoded as ziplists at 1000 entries or smaller, Redis is still able to perform around 50,000 operations per second or better
#B But as lists encoded as ziplists grow to 5000 or more, performance starts to drop off as memory copy costs start reducing performance
#C Once we hit 50,000 entries in a ziplist, performance has dropped significantly
#D And once we hit 100,000 entries, ziplists are effectively unusable
#END
'''

def long_ziplist_index(conn, key, length, passes, psize): #A
    conn.delete(key)                    #B
    conn.rpush(key, *range(length))     #C
    length >>= 1
    pipeline = conn.pipeline(False)     #D
    t = time.time()                     #E
    for p in xrange(passes):            #F
        for pi in xrange(psize):        #G
            pipeline.lindex(key, length)#H
        pipeline.execute()              #I
    return (passes * psize) / (time.time() - t or .001) #J

def long_intset_performance(conn, key, length, passes, psize): #A
    conn.delete(key)                    #B
    conn.sadd(key, *range(1000000, 1000000+length))     #C
    cur = 1000000-1
    pipeline = conn.pipeline(False)     #D
    t = time.time()                     #E
    for p in xrange(passes):            #F
        for pi in xrange(psize):        #G
            pipeline.spop(key)#H
            pipeline.sadd(key, cur)
            cur -= 1
        pipeline.execute()              #I
    return (passes * psize) / (time.time() - t or .001) #J


# <start id="calculate-shard-key"/>
def shard_key(base, key, total_elements, shard_size):   #A
    if isinstance(key, (int, long)) or key.isdigit():   #B
        shard_id = int(str(key), 10) // shard_size      #C
    else:
        shards = 2 * total_elements // shard_size       #D
        shard_id = binascii.crc32(key) % shards         #E
    return "%s:%s"%(base, shard_id)                     #F
# <end id="calculate-shard-key"/>
#A We will call the shard_key() function with a base HASH name, along with the key to be stored in the sharded HASH, the total number of expected elements, and the desired shard size
#B If the value is an integer or a string that looks like an integer, we will use it directly to calculate the shard id
#C For integers, we assume they are sequentially assigned ids, so we can choose a shard id based on the upper 'bits' of the numeric id itself. We also use an explicit base here (necessitating the str() call) so that a key of '010' turns into 10, and not 8
#D For non-integer keys, we first calculate the total number of shards desired, based on an expected total number of elements and desired shard size
#E When we know the number of shards we want, we hash the key and find its value modulo the number of shards we want
#F Finally, we combine the base key with the shard id we calculated to determine the shard key
#END

# <start id="sharded-hset-hget"/>
def shard_hset(conn, base, key, value, total_elements, shard_size):
    shard = shard_key(base, key, total_elements, shard_size)    #A
    return conn.hset(shard, key, value)                         #B

def shard_hget(conn, base, key, total_elements, shard_size):
    shard = shard_key(base, key, total_elements, shard_size)    #C
    return conn.hget(shard, key)                                #D
# <end id="sharded-hset-hget"/>
#A Calculate the shard to store our value in
#B Set the value in the shard
#C Calculate the shard to fetch our value from
#D Get the value in the shard
#END

'''
# <start id="sharded-ip-lookup"/>
TOTAL_SIZE = 320000                                             #A
SHARD_SIZE = 1024                                               #A

def import_cities_to_redis(conn, filename):
    for row in csv.reader(open(filename)):
        ...
        shard_hset(conn, 'cityid2city:', city_id,               #B
            json.dumps([city, region, country]),                #B
            TOTAL_SIZE, SHARD_SIZE)                             #B

def find_city_by_ip(conn, ip_address):
    ...
    data = shard_hget(conn, 'cityid2city:', city_id,            #C
        TOTAL_SIZE, SHARD_SIZE)                                 #C
    return json.loads(data)
# <end id="sharded-ip-lookup"/>
#A We set the arguments for the sharded calls as global constants to ensure that we always pass the same information
#B To set the data, we need to pass the TOTAL_SIZE and SHARD_SIZE information, though in this case TOTAL_SIZE is unused because our ids are numeric
#C To fetch the data, we need to use the same information for TOTAL_SIZE and SHARD_SIZE for general sharded keys
#END
'''

# <start id="sharded-sadd"/>
def shard_sadd(conn, base, member, total_elements, shard_size):
    shard = shard_key(base,
        'x'+str(member), total_elements, shard_size)            #A
    return conn.sadd(shard, member)                             #B
# <end id="sharded-sadd"/>
#A Shard the member into one of the sharded SETs, remember to turn it into a string because it isn't a sequential id
#B Actually add the member to the shard
#END

# <start id="unique-visitor-count"/>
SHARD_SIZE = 512                        #B

def count_visit(conn, session_id):
    today = date.today()                                #C
    key = 'unique:%s'%today.isoformat()                 #C
    expected = get_expected(conn, key, today)           #D
 
    id = int(session_id.replace('-', '')[:15], 16)      #E
    if shard_sadd(conn, key, id, expected, SHARD_SIZE): #F
        conn.incr(key)                                  #G
# <end id="unique-visitor-count"/>
#B And we stick with a typical shard size for the intset encoding for SETs
#C Get today's date and generate the key for the unique count
#D Fetch or calculate the expected number of unique views today
#E Calculate the 56 bit id for this 128 bit UUID
#F Add the id to the sharded SET
#G If the id wasn't in the sharded SET, then we increment our uniqie view count
#END

# <start id="expected-viewer-count"/>
DAILY_EXPECTED = 1000000                                #I
EXPECTED = {}                                           #A

def get_expected(conn, key, today):
    if key in EXPECTED:                                 #B
        return EXPECTED[key]                            #B
 
    exkey = key + ':expected'
    expected = conn.get(exkey)                          #C
 
    if not expected:
        yesterday = (today - timedelta(days=1)).isoformat() #D
        expected = conn.get('unique:%s'%yesterday)          #D
        expected = int(expected or DAILY_EXPECTED)          #D
 
        expected = 2**int(math.ceil(math.log(expected*1.5, 2))) #E
        if not conn.setnx(exkey, expected):                 #F
            expected = conn.get(exkey)                      #G
 
    EXPECTED[key] = int(expected)                       #H
    return EXPECTED[key]                                #H
# <end id="expected-viewer-count"/>
#I We start with an initial expected number of daily visits that may be a little high
#A Keep a local copy of any calculated expected counts
#B If we have already calculated or seen the expected number of views for today, use that number
#C If someone else has already calculated the expected number of views for today, use that number
#D Fetch the unique count for yesterday, or if not available, use our default 1 million
#E Add 50% to yesterday's count, and round up to the next even power of 2, under the assumption that view count today should be at least 50% better than yesterday
#F Save our calculated expected number of views back to Redis for other calls if possible
#G If someone else stored the expected count for today before us, use their count instead
#H Keep a local copy of today's expected number of hits, and return it back to the caller
#END

# <start id="location-tables"/>
COUNTRIES = '''
ABW AFG AGO AIA ALA ALB AND ARE ARG ARM ASM ATA ATF ATG AUS AUT AZE BDI
BEL BEN BES BFA BGD BGR BHR BHS BIH BLM BLR BLZ BMU BOL BRA BRB BRN BTN
BVT BWA CAF CAN CCK CHE CHL CHN CIV CMR COD COG COK COL COM CPV CRI CUB
CUW CXR CYM CYP CZE DEU DJI DMA DNK DOM DZA ECU EGY ERI ESH ESP EST ETH
FIN FJI FLK FRA FRO FSM GAB GBR GEO GGY GHA GIB GIN GLP GMB GNB GNQ GRC
GRD GRL GTM GUF GUM GUY HKG HMD HND HRV HTI HUN IDN IMN IND IOT IRL IRN
IRQ ISL ISR ITA JAM JEY JOR JPN KAZ KEN KGZ KHM KIR KNA KOR KWT LAO LBN
LBR LBY LCA LIE LKA LSO LTU LUX LVA MAC MAF MAR MCO MDA MDG MDV MEX MHL
MKD MLI MLT MMR MNE MNG MNP MOZ MRT MSR MTQ MUS MWI MYS MYT NAM NCL NER
NFK NGA NIC NIU NLD NOR NPL NRU NZL OMN PAK PAN PCN PER PHL PLW PNG POL
PRI PRK PRT PRY PSE PYF QAT REU ROU RUS RWA SAU SDN SEN SGP SGS SHN SJM
SLB SLE SLV SMR SOM SPM SRB SSD STP SUR SVK SVN SWE SWZ SXM SYC SYR TCA
TCD TGO THA TJK TKL TKM TLS TON TTO TUN TUR TUV TWN TZA UGA UKR UMI URY
USA UZB VAT VCT VEN VGB VIR VNM VUT WLF WSM YEM ZAF ZMB ZWE'''.split()#A

STATES = {
    'CAN':'''AB BC MB NB NL NS NT NU ON PE QC SK YT'''.split(),       #B
    'USA':'''AA AE AK AL AP AR AS AZ CA CO CT DC DE FL FM GA GU HI IA ID
IL IN KS KY LA MA MD ME MH MI MN MO MP MS MT NC ND NE NH NJ NM NV NY OH
OK OR PA PR PW RI SC SD TN TX UT VA VI VT WA WI WV WY'''.split(),     #C
}
# <end id="location-tables"/>
#A A table of ISO 3 country codes. Calling 'split()' will split the string on whitespace, turning the string into a list of country codes
#B Province/territory information for Canada
#C State information for the United States
#END

# <start id="location-to-code"/>
def get_code(country, state):
    cindex = bisect.bisect_left(COUNTRIES, country)             #A
    if cindex > len(COUNTRIES) or COUNTRIES[cindex] != country: #B
        cindex = -1                                             #B
    cindex += 1                                                 #C

    sindex = -1
    if state and country in STATES:
        states = STATES[country]                                #D
        sindex = bisect.bisect_left(states, state)              #E
        if sindex > len(states) or states[sindex] != state:     #F
            sindex = -1                                         #F
    sindex += 1                                                 #G

    return chr(cindex) + chr(sindex)                            #H
# <end id="location-to-code"/>
#A Find the offset for the country
#B If the country isn't found, then set its index to be -1
#C Because uninitialized data in Redis will return as nulls, we want 'not found' to be 0, and the first country to be 1
#D Pull the state information for the country, if it is available
#E Find the offset for the state
#F Handle not-found states like we did with countries
#G Keep not-found states at 0, and found states > 0
#H The chr() function will turn an integer value of 0..255 into the ascii character with that same value
#END

# <start id="set-location-information"/>
USERS_PER_SHARD = 2**20                                     #A

def set_location(conn, user_id, country, state):
    code = get_code(country, state)                         #B
    
    shard_id, position = divmod(user_id, USERS_PER_SHARD)   #C
    offset = position * 2                                   #D

    pipe = conn.pipeline(False)
    pipe.setrange('location:%s'%shard_id, offset, code)     #E

    tkey = str(uuid.uuid4())                                #F
    pipe.zadd(tkey, 'max', user_id)                         #F
    pipe.zunionstore('location:max',                        #F
        [tkey, 'location:max'], aggregate='max')            #F
    pipe.delete(tkey)                                       #F

    pipe.execute()
# <end id="set-location-information"/>
#A Set the size of each shard
#B Get the location code to store for the user
#C Find the shard id and position of the user in the specific shard
#D Calculate the offset of the user's data
#E Set the value in the proper sharded location table
#F Update a ZSET that stores the maximum user id seen so far
#END

# <start id="aggregate-population"/>
def aggregate_location(conn):
    countries = defaultdict(int)                                #A
    states = defaultdict(lambda:defaultdict(int))               #A

    max_id = int(conn.zscore('location:max', 'max'))            #B
    max_block = max_id // USERS_PER_SHARD                       #B

    for shard_id in xrange(max_block + 1):                      #C
        for block in readblocks(conn, 'location:%s'%shard_id):  #D
            for offset in xrange(0, len(block)-1, 2):           #E
                code = block[offset:offset+2]
                update_aggregates(countries, states, [code])    #F

    return countries, states
# <end id="aggregate-population"/>
#A Initialize two special structures that will allow us to quickly update existing and missing counters quickly
#B Fetch the maximum user id known, and use that to calculate the maximum shard id that we need to visit
#C Sequentially check every shard
#D ... reading each block
#E Extract each code from the block and look up the original location information (like USA, CA for someone who lives in California)
#F Update our aggregates
#END

# <start id="code-to-location"/>
def update_aggregates(countries, states, codes):
    for code in codes:
        if len(code) != 2:                              #A
            continue                                    #A

        country = ord(code[0]) - 1                      #B
        state = ord(code[1]) - 1                        #B
        
        if country < 0 or country >= len(COUNTRIES):    #C
            continue                                    #C

        country = COUNTRIES[country]                    #D
        countries[country] += 1                         #E

        if country not in STATES:                       #F
            continue                                    #F
        if state < 0 or state >= len(STATES[country]):  #F
            continue                                    #F

        state = STATES[country][state]                  #G
        states[country][state] += 1                     #H
# <end id="code-to-location"/>
#A Only look up codes that could be valid
#B Calculate the actual offset of the country and state in the lookup tables
#C If the country is out of the range of valid countries, continue to the next code
#D Fetch the ISO-3 country code
#E Count this user in the decoded country
#F If we don't have state information or if the state is out of the range of valid states for the country, continue to the next code
#G Fetch the state name from the code
#H Increment the count for the state
#END

# <start id="aggregate-limited"/>
def aggregate_location_list(conn, user_ids):
    pipe = conn.pipeline(False)                                 #A
    countries = defaultdict(int)                                #B
    states = defaultdict(lambda: defaultdict(int))              #B

    for i, user_id in enumerate(user_ids):
        shard_id, position = divmod(user_id, USERS_PER_SHARD)   #C
        offset = position * 2                                   #C

        pipe.substr('location:%s'%shard_id, offset, offset+1)   #D

        if (i+1) % 1000 == 0:                                   #E
            update_aggregates(countries, states, pipe.execute())#E

    update_aggregates(countries, states, pipe.execute())        #F

    return countries, states                                    #G
# <end id="aggregate-limited"/>
#A Set up the pipeline so that we aren't making too many round-trips to Redis
#B Set up our base aggregates as we did before
#C Calculate the shard id and offset into the shard for this user's location
#D Send another pipelined command to fetch the location information for the user
#E Every 1000 requests, we will actually update the aggregates using the helper function we defined before
#F Handle the last hunk of users that we might have missed before
#G Return the aggregates
#END

class TestCh09(unittest.TestCase):
    def setUp(self):
        self.conn = redis.Redis(db=15)
        self.conn.flushdb()
    def tearDown(self):
        self.conn.flushdb()

    def test_long_ziplist_performance(self):
        long_ziplist_performance(self.conn, 'test', 5, 10, 10)
        self.assertEquals(self.conn.llen('test'), 5)

    def test_shard_key(self):
        base = 'test'
        self.assertEquals(shard_key(base, 1, 2, 2), 'test:0')
        self.assertEquals(shard_key(base, '1', 2, 2), 'test:0')
        self.assertEquals(shard_key(base, 125, 1000, 100), 'test:1')
        self.assertEquals(shard_key(base, '125', 1000, 100), 'test:1')

        for i in xrange(50):
            self.assertTrue(0 <= int(shard_key(base, 'hello:%s'%i, 1000, 100).partition(':')[-1]) < 20)
            self.assertTrue(0 <= int(shard_key(base, i, 1000, 100).partition(':')[-1]) < 10)

    def test_sharded_hash(self):
        for i in xrange(50):
            shard_hset(self.conn, 'test', 'keyname:%s'%i, i, 1000, 100)
            self.assertEquals(shard_hget(self.conn, 'test', 'keyname:%s'%i, 1000, 100), str(i))
            shard_hset(self.conn, 'test2', i, i, 1000, 100)
            self.assertEquals(shard_hget(self.conn, 'test2', i, 1000, 100), str(i))

    def test_sharded_sadd(self):
        for i in xrange(50):
            shard_sadd(self.conn, 'testx', i, 50, 50)
        self.assertEquals(self.conn.scard('testx:0') + self.conn.scard('testx:1'), 50)

    def test_unique_visitors(self):
        global DAILY_EXPECTED
        DAILY_EXPECTED = 10000
        
        for i in xrange(179):
            count_visit(self.conn, str(uuid.uuid4()))
        self.assertEquals(self.conn.get('unique:%s'%(date.today().isoformat())), '179')

        self.conn.flushdb()
        self.conn.set('unique:%s'%((date.today() - timedelta(days=1)).isoformat()), 1000)
        for i in xrange(183):
            count_visit(self.conn, str(uuid.uuid4()))
        self.assertEquals(self.conn.get('unique:%s'%(date.today().isoformat())), '183')

    def test_user_location(self):
        i = 0
        for country in COUNTRIES:
            if country in STATES:
                for state in STATES[country]:
                    set_location(self.conn, i, country, state)
                    i += 1
            else:
                set_location(self.conn, i, country, '')
                i += 1
        
        _countries, _states = aggregate_location(self.conn)
        countries, states = aggregate_location_list(self.conn, range(i+1))
        
        self.assertEquals(_countries, countries)
        self.assertEquals(_states, states)

        for c in countries:
            if c in STATES:
                self.assertEquals(len(STATES[c]), countries[c])
                for s in STATES[c]:
                    self.assertEquals(states[c][s], 1)
            else:
                self.assertEquals(countries[c], 1)

if __name__ == '__main__':
    unittest.main()
