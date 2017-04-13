
import binascii
from collections import defaultdict
from datetime import date
from decimal import Decimal
import functools
import json
from Queue import Empty, Queue
import threading
import time
import unittest
import uuid

import redis

CONFIGS = {}
CHECKED = {}

def get_config(conn, type, component, wait=1):
    key = 'config:%s:%s'%(type, component)

    if CHECKED.get(key) < time.time() - wait:           #A
        CHECKED[key] = time.time()                      #B
        config = json.loads(conn.get(key) or '{}')      #C
        config = dict((str(k), config[k]) for k in config)
        old_config = CONFIGS.get(key)                   #D

        if config != old_config:                        #E
            CONFIGS[key] = config                       #F

    return CONFIGS.get(key)

REDIS_CONNECTIONS = {}
config_connection = None

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

def index_document(conn, docid, words, scores):
    pipeline = conn.pipeline(True)
    for word in words:                                                  #I
        pipeline.sadd('idx:' + word, docid)                             #I
    pipeline.hmset('kb:doc:%s'%docid, scores)
    return len(pipeline.execute())                                      #J

def parse_and_search(conn, query, ttl):
    id = str(uuid.uuid4())
    conn.sinterstore('idx:' + id,
        ['idx:'+key for key in query])
    conn.expire('idx:' + id, ttl)
    return id

def search_and_sort(conn, query, id=None, ttl=300, sort="-updated", #A
                    start=0, num=20):                               #A
    desc = sort.startswith('-')                                     #B
    sort = sort.lstrip('-')                                         #B
    by = "kb:doc:*->" + sort                                        #B
    alpha = sort not in ('updated', 'id', 'created')                #I

    if id and not conn.expire(id, ttl):     #C
        id = None                           #C

    if not id:                                      #D
        id = parse_and_search(conn, query, ttl=ttl) #D

    pipeline = conn.pipeline(True)
    pipeline.scard('idx:' + id)                                     #E
    pipeline.sort('idx:' + id, by=by, alpha=alpha,                  #F
        desc=desc, start=start, num=num)                            #F
    results = pipeline.execute()

    return results[0], results[1], id                               #G

def zintersect(conn, keys, ttl):
    id = str(uuid.uuid4())
    conn.zinterstore('idx:' + id,
        dict(('idx:'+k, v) for k,v in keys.iteritems()))
    conn.expire('idx:' + id, ttl)
    return id

def search_and_zsort(conn, query, id=None, ttl=300, update=1, vote=0,   #A
                    start=0, num=20, desc=True):                        #A

    if id and not conn.expire(id, ttl):     #B
        id = None                           #B

    if not id:                                      #C
        id = parse_and_search(conn, query, ttl=ttl) #C

        scored_search = {                           #D
            id: 0,                                  #D
            'sort:update': update,                  #D
            'sort:votes': vote                      #D
        }
        id = zintersect(conn, scored_search, ttl)   #E

    pipeline = conn.pipeline(True)
    pipeline.zcard('idx:' + id)                                     #F
    if desc:                                                        #G
        pipeline.zrevrange('idx:' + id, start, start + num - 1)     #G
    else:                                                           #G
        pipeline.zrange('idx:' + id, start, start + num - 1)        #G
    results = pipeline.execute()

    return results[0], results[1], id                               #H

def execute_later(conn, queue, name, args):
    t = threading.Thread(target=globals()[name], args=tuple(args))
    t.setDaemon(1)
    t.start()

HOME_TIMELINE_SIZE = 1000
POSTS_PER_PASS = 1000

def shard_key(base, key, total_elements, shard_size):   #A
    if isinstance(key, (int, long)) or key.isdigit():   #B
        shard_id = int(str(key), 10) // shard_size      #C
    else:
        shards = 2 * total_elements // shard_size       #D
        shard_id = binascii.crc32(key) % shards         #E
    return "%s:%s"%(base, shard_id)                     #F

def shard_sadd(conn, base, member, total_elements, shard_size):
    shard = shard_key(base,
        'x'+str(member), total_elements, shard_size)            #A
    return conn.sadd(shard, member)                             #B

SHARD_SIZE = 512
EXPECTED = defaultdict(lambda: 1000000)

# <start id="get-connection"/>
def get_redis_connection(component, wait=1):
    key = 'config:redis:' + component
    old_config = CONFIGS.get(key, object())             #A
    config = get_config(                                #B
        config_connection, 'redis', component, wait)    #B

    if config != old_config:                            #C
        REDIS_CONNECTIONS[key] = redis.Redis(**config)  #C

    return REDIS_CONNECTIONS.get(key)                   #D
# <end id="get-connection"/>
#A Fetch the old configuration, if any
#B Get the new configuration, if any
#C If the new and old configuration do not match, create a new connection
#D Return the desired connection object
#END

# <start id="get-sharded-connection"/>
def get_sharded_connection(component, key, shard_count, wait=1):
    shard = shard_key(component, 'x'+str(key), shard_count, 2)  #A
    return get_redis_connection(shard, wait)                    #B
# <end id="get-sharded-connection"/>
#A Calculate the shard id of the form: &lt;component&gt;:&lt;shard&gt;
#B Return the connection
#END


# <start id="no-decorator-example"/>
def log_recent(conn, app, message):
    'the old log_recent() code'

log_recent = redis_connection('logs')(log_recent)   #A
# <end id="no-decorator-example"/>
#A This performs the equivalent decoration, but requires repeating the 'log_recent' function name 3 times
#END

# <start id="shard-aware-decorator"/>
def sharded_connection(component, shard_count, wait=1):         #A
    def wrapper(function):                                      #B
        @functools.wraps(function)                              #C
        def call(key, *args, **kwargs):                         #D
            conn = get_sharded_connection(                      #E
                component, key, shard_count, wait)              #E
            return function(conn, key, *args, **kwargs)         #F
        return call                                             #G
    return wrapper                                              #H
# <end id="shard-aware-decorator"/>
#A Our decorator is going to take a component name, as well as the number of shards desired
#B We are then going to create a wrapper that will actually decorate the function
#C Copy some useful metadata from the original function to the configuration handler
#D Create the function that will calculate a shard id for keys, and set up the connection manager
#E Fetch the sharded connection
#F Actually call the function, passing the connection and existing arguments
#G Return the fully wrapped function
#H Return a function that can wrap functions that need a sharded connection
#END

# <start id="sharded-count-unique"/>
@sharded_connection('unique', 16)                       #A
def count_visit(conn, session_id):
    today = date.today()
    key = 'unique:%s'%today.isoformat()
    conn2, expected = get_expected(key, today)          #B

    id = int(session_id.replace('-', '')[:15], 16)
    if shard_sadd(conn, key, id, expected, SHARD_SIZE):
        conn2.incr(key)                                 #C

@redis_connection('unique')                             #D
def get_expected(conn, key, today):
    'all of the same function body as before, except the last line'
    return conn, EXPECTED[key]                          #E
# <end id="sharded-count-unique"/>
#A We are going to shard this to 16 different machines, which will automatically shard to multiple keys on each machine
#B Our changed call to get_expected()
#C Use the returned non-sharded connection to increment our unique counts
#D Use a non-sharded connection to get_expected()
#E Also return the non-sharded connection so that count_visit() can increment our unique count as necessary
#END

# <start id="search-with-values"/>
def search_get_values(conn, query, id=None, ttl=300, sort="-updated", #A
                      start=0, num=20):                               #A
    count, docids, id = search_and_sort(                            #B
        conn, query, id, ttl, sort, 0, start+num)                   #B

    key = "kb:doc:%s"
    sort = sort.lstrip('-')

    pipe = conn.pipeline(False)
    for docid in docids:                                            #C
        pipe.hget(key%docid, sort)                                  #C
    sort_column = pipe.execute()                                    #C

    data_pairs = zip(docids, sort_column)                           #D
    return count, data_pairs, id                                    #E
# <end id="search-with-values"/>
#A We need to take all of the same parameters to pass on to search_and_sort()
#B First get the results of a search and sort
#C Fetch the data that the results were sorted by
#D Pair up the document ids with the data that it was sorted by
#E Return the count, data, and cache id of the results
#END

# <start id="search-on-shards"/>
def get_shard_results(component, shards, query, ids=None, ttl=300,  #A
                  sort="-updated", start=0, num=20, wait=1):        #A

    count = 0       #B
    data = []       #B
    ids = ids or shards * [None]       #C
    for shard in xrange(shards):
        conn = get_redis_connection('%s:%s'%(component, shard), wait)#D
        c, d, i = search_get_values(                        #E
            conn, query, ids[shard], ttl, sort, start, num) #E

        count += c          #F
        data.extend(d)      #F
        ids[shard] = i      #F

    return count, data, ids     #G
# <end id="search-on-shards"/>
#A In order to know what servers to connect to, we are going to assume that all of our shard information is kept in the standard configuration location
#B Prepare structures to hold all of our fetched data
#C Use cached results if we have any, otherwise start over
#D Get or create a connection to the desired shard
#E Fetch the search results and their sort values
#F Combine this shard's results with all of the other results
#G Return the raw results from all of the shards
#END

def get_values_thread(component, shard, wait, rqueue, *args, **kwargs):
    conn = get_redis_connection('%s:%s'%(component, shard), wait)
    count, results, id = search_get_values(conn, *args, **kwargs)
    rqueue.put((shard, count, results, id))

def get_shard_results_thread(component, shards, query, ids=None, ttl=300,
                  sort="-updated", start=0, num=20, wait=1, timeout=.5):

    ids = ids or shards * [None]
    rqueue = Queue()

    for shard in xrange(shards):
        t = threading.Thread(target=get_values_thread, args=(
            component, shard, wait, rqueue, query, ids[shard],
            ttl, sort, start, num))
        t.setDaemon(1)
        t.start()

    received = 0
    count = 0
    data = []
    deadline = time.time() + timeout
    while received < shards and time.time() < deadline:
        try:
            sh, c, r, i = rqueue.get(timeout=max(deadline-time.time(), .001))
        except Empty:
            break
        else:
            count += c
            data.extend(r)
            ids[sh] = i

    return count, data, ids

# <start id="merge-sharded-results"/>
def to_numeric_key(data):
    try:
        return Decimal(data[1] or '0')      #A
    except:
        return Decimal('0')                 #A

def to_string_key(data):
    return data[1] or ''                    #B

def search_shards(component, shards, query, ids=None, ttl=300,      #C
                  sort="-updated", start=0, num=20, wait=1):        #C

    count, data, ids = get_shard_results(                           #D
        component, shards, query, ids, ttl, sort, start, num, wait) #D

    reversed = sort.startswith('-')                     #E
    sort = sort.strip('-')                              #E
    key = to_numeric_key                                #E
    if sort not in ('updated', 'id', 'created'):        #E
        key = to_string_key                             #E

    data.sort(key=key, reverse=reversed)               #F

    results = []
    for docid, score in data[start:start+num]:          #G
        results.append(docid)                           #G

    return count, results, ids                          #H
# <end id="merge-sharded-results"/>
#A We are going to use the 'Decimal' numeric type here because it transparently handles both integers and floats reasonably, defaulting to 0 if the value wasn't numeric or was missing
#B Always return a string, even if there was no value stored
#C We need to take all of the sharding and searching arguments, mostly to pass on to lower-level functions, but we use the sort and search offsets
#D Fetch the results of the unsorted sharded search
#E Prepare all of our sorting options
#F Actually sort our results based on the sort parameter
#G Fetch just the page of results that we want
#H Return the results, including the sequence of cache ids for each shard
#END

# <start id="zset-search-with-values"/>
def search_get_zset_values(conn, query, id=None, ttl=300, update=1, #A
                    vote=0, start=0, num=20, desc=True):            #A

    count, r, id = search_and_zsort(                                #B
        conn, query, id, ttl, update, vote, 0, 1, desc)             #B

    if desc:                                                        #C
        data = conn.zrevrange(id, 0, start + num - 1, withscores=True)#C
    else:                                                           #C
        data = conn.zrange(id, 0, start + num - 1, withscores=True) #C

    return count, data, id                                          #D
# <end id="zset-search-with-values"/>
#A We need to accept all of the standard arguments for search_and_zsort()
#B Call the underlying search_and_zsort() function to get the cached result id and total number of results
#C Fetch all of the results we need, including their scores
#D Return the count, results with scores, and the cache id
#END

# <start id="search-shards-zset"/>
def search_shards_zset(component, shards, query, ids=None, ttl=300,   #A
                update=1, vote=0, start=0, num=20, desc=True, wait=1):#A

    count = 0                       #B
    data = []                       #B
    ids = ids or shards * [None]    #C
    for shard in xrange(shards):
        conn = get_redis_connection('%s:%s'%(component, shard), wait) #D
        c, d, i = search_get_zset_values(conn, query, ids[shard],     #E
            ttl, update, vote, start, num, desc)                      #E

        count += c      #F
        data.extend(d)  #F
        ids[shard] = i  #F

    def key(result):        #G
        return result[1]    #G

    data.sort(key=key, reversed=desc)   #H
    results = []
    for docid, score in data[start:start+num]:  #I
        results.append(docid)                   #I

    return count, results, ids                  #J
# <end id="search-shards-zset"/>
#A We need to take all of the sharding arguments along with all of the search arguments
#B Prepare structures for data to be returned
#C Use cached results if any, otherwise start from scratch
#D Fetch or create a connection to each shard
#E Perform the search on a shard and fetch the scores
#F Merge the results together
#G Prepare the simple sort helper to only return information about the score
#H Sort all of the results together
#I Extract the document ids from the results, removing the scores
#J Return the search results to the caller
#END

# <start id="sharded-api-base"/>
class KeyShardedConnection(object):
    def __init__(self, component, shards):          #A
        self.component = component                  #A
        self.shards = shards                        #A
    def __getitem__(self, key):                     #B
        return get_sharded_connection(              #C
            self.component, key, self.shards)       #C
# <end id="sharded-api-base"/>
#A The object is initialized with the component name and number of shards
#B When an item is fetched from the object, this method is called with the item that was requested
#C Use the passed key along with the previously-known component and shards to fetch the sharded connection
#END

# <start id="sharded-api-example"/>
sharded_timelines = KeyShardedConnection('timelines', 8)    #A

def follow_user(conn, uid, other_uid):
    fkey1 = 'following:%s'%uid
    fkey2 = 'followers:%s'%other_uid

    if conn.zscore(fkey1, other_uid):
        print "already followed", uid, other_uid
        return None

    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zadd(fkey1, other_uid, now)
    pipeline.zadd(fkey2, uid, now)
    pipeline.zcard(fkey1)
    pipeline.zcard(fkey2)
    following, followers = pipeline.execute()[-2:]
    pipeline.hset('user:%s'%uid, 'following', following)
    pipeline.hset('user:%s'%other_uid, 'followers', followers)
    pipeline.execute()

    pkey = 'profile:%s'%other_uid
    status_and_score = sharded_timelines[pkey].zrevrange(   #B
        pkey, 0, HOME_TIMELINE_SIZE-1, withscores=True)     #B

    if status_and_score:
        hkey = 'home:%s'%uid
        pipe = sharded_timelines[hkey].pipeline(True)       #C
        pipe.zadd(hkey, **dict(status_and_score))           #D
        pipe.zremrangebyrank(hkey, 0, -HOME_TIMELINE_SIZE-1)#D
        pipe.execute()                                      #E

    return True
# <end id="sharded-api-example"/>
#A Create a connection that knows about the sharding information for a given component with a number of shards
#B Fetch the recent status messages from the profile timeline of the now-followed user
#C Get a connection based on the shard key provided, and fetch a pipeline from that
#D Add the statuses to the home timeline ZSET on the shard, then trim it
#E Execute the transaction
#END


# <start id="key-data-sharded-api"/>
class KeyDataShardedConnection(object):
    def __init__(self, component, shards):          #A
        self.component = component                  #A
        self.shards = shards                        #A
    def __getitem__(self, ids):                     #B
        id1, id2 = map(int, ids)                    #C
        if id2 < id1:                               #D
            id1, id2 = id2, id1                     #D
        key = "%s:%s"%(id1, id2)                    #E
        return get_sharded_connection(              #F
            self.component, key, self.shards)       #F
# <end id="key-data-sharded-api"/>
#A The object is initialized with the component name and number of shards
#B When the pair of ids are passed as part of the dictionary lookup, this method is called
#C Unpack the pair of ids, and ensure that they are integers
#D If the second is less than the first, swap them so that the first id is less than or equal to the second
#E Construct a key based on the two ids
#F Use the computed key along with the previously-known component and shards to fetch the sharded connection
#END

_follow_user = follow_user
# <start id="sharded-api-example2"/>
sharded_timelines = KeyShardedConnection('timelines', 8)        #A
sharded_followers = KeyDataShardedConnection('followers', 16)   #A

def follow_user(conn, uid, other_uid):
    fkey1 = 'following:%s'%uid
    fkey2 = 'followers:%s'%other_uid

    sconn = sharded_followers[uid, other_uid]           #B
    if sconn.zscore(fkey1, other_uid):                  #C
        return None

    now = time.time()
    spipe = sconn.pipeline(True)
    spipe.zadd(fkey1, other_uid, now)                   #D
    spipe.zadd(fkey2, uid, now)                         #D
    following, followers = spipe.execute()

    pipeline = conn.pipeline(True)
    pipeline.hincrby('user:%s'%uid, 'following', int(following))      #E
    pipeline.hincrby('user:%s'%other_uid, 'followers', int(followers))#E
    pipeline.execute()

    pkey = 'profile:%s'%other_uid
    status_and_score = sharded_timelines[pkey].zrevrange(
        pkey, 0, HOME_TIMELINE_SIZE-1, withscores=True)

    if status_and_score:
        hkey = 'home:%s'%uid
        pipe = sharded_timelines[hkey].pipeline(True)
        pipe.zadd(hkey, **dict(status_and_score))
        pipe.zremrangebyrank(hkey, 0, -HOME_TIMELINE_SIZE-1)
        pipe.execute()

    return True
# <end id="sharded-api-example2"/>
#A Create a connection that knows about the sharding information for a given component with a number of shards
#B Fetch the connection object for the uid,other_uid pair
#C Check to see if other_uid is already followed
#D Add the follower/following information to the ZSETs
#E Update the follower and following information for both users
#END

# <start id="sharded-zrangebyscore"/>
def sharded_zrangebyscore(component, shards, key, min, max, num):   #A
    data = []
    for shard in xrange(shards):
        conn = get_redis_connection("%s:%s"%(component, shard))     #B
        data.extend(conn.zrangebyscore(                             #C
            key, min, max, start=0, num=num, withscores=True))      #C

    def key(pair):                      #D
        return pair[1], pair[0]         #D
    data.sort(key=key)                  #D

    return data[:num]                   #E
# <end id="sharded-zrangebyscore"/>
#A We need to take arguments for the component and number of shards, and we are going to limit the arguments to be passed on to only those that will ensure correct behavior in sharded situations
#B Fetch the sharded connection for the current shard
#C Get the data from Redis for this shard
#D Sort the data based on score then by member
#E Return only the number of items requested
#END

# <start id="sharded-syndicate-posts"/>
def syndicate_status(uid, post, start=0, on_lists=False):
    root = 'followers'
    key = 'followers:%s'%uid
    base = 'home:%s'
    if on_lists:
        root = 'list:out'
        key = 'list:out:%s'%uid
        base = 'list:statuses:%s'

    followers = sharded_zrangebyscore(root,                         #A
        sharded_followers.shards, key, start, 'inf', POSTS_PER_PASS)#A

    to_send = defaultdict(list)                             #B
    for follower, start in followers:
        timeline = base % follower                          #C
        shard = shard_key('timelines',                      #D
            timeline, sharded_timelines.shards, 2)          #D
        to_send[shard].append(timeline)                     #E

    for timelines in to_send.itervalues():
        pipe = sharded_timelines[timelines[0]].pipeline(False)  #F
        for timeline in timelines:
            pipe.zadd(timeline, **post)                 #G
            pipe.zremrangebyrank(                       #G
                timeline, 0, -HOME_TIMELINE_SIZE-1)     #G
        pipe.execute()

    conn = redis.Redis()
    if len(followers) >= POSTS_PER_PASS:
        execute_later(conn, 'default', 'syndicate_status',
            [uid, post, start, on_lists])

    elif not on_lists:
        execute_later(conn, 'default', 'syndicate_status',
            [uid, post, 0, True])
# <end id="sharded-syndicate-posts"/>
#A Fetch the next group of followers using the sharded ZRANGEBYSCORE call
#B Prepare a structure that will group profile information on a per-shard basis
#C Calculate the key for the timeline
#D Find the shard where this timeline would go
#E Add the timeline key to the rest of the timelines on the same shard
#F Get a connection to the server for the group of timelines, and create a pipeline
#G Add the post to the timeline, and remove any posts that are too old
#END

def _fake_shards_for(conn, component, count, actual):
    assert actual <= 4
    for i in xrange(count):
        m = i % actual
        conn.set('config:redis:%s:%i'%(component, i), json.dumps({'db':14 - m}))

class TestCh10(unittest.TestCase):
    def _flush(self):
        self.conn.flushdb()
        redis.Redis(db=14).flushdb()
        redis.Redis(db=13).flushdb()
        redis.Redis(db=12).flushdb()
        redis.Redis(db=11).flushdb()
        
    def setUp(self):
        self.conn = redis.Redis(db=15)
        self._flush()
        global config_connection
        config_connection = self.conn
        self.conn.set('config:redis:test', json.dumps({'db':15}))

    def tearDown(self):
        self._flush()

    def test_get_sharded_connections(self):
        _fake_shards_for(self.conn, 'shard', 2, 2)

        for i in xrange(10):
            get_sharded_connection('shard', i, 2).sadd('foo', i)

        s0 = redis.Redis(db=14).scard('foo')
        s1 = redis.Redis(db=13).scard('foo')
        self.assertTrue(s0 < 10)
        self.assertTrue(s1 < 10)
        self.assertEquals(s0 + s1, 10)

    def test_count_visit(self):
        shards = {'db':13}, {'db':14}
        self.conn.set('config:redis:unique', json.dumps({'db':15}))
        for i in xrange(16):
            self.conn.set('config:redis:unique:%s'%i, json.dumps(shards[i&1]))
    
        for i in xrange(100):
            count_visit(str(uuid.uuid4()))
        base = 'unique:%s'%date.today().isoformat()
        total = 0
        for c in shards:
            conn = redis.Redis(**c)
            keys = conn.keys(base + ':*')
            for k in keys:
                cnt = conn.scard(k)
                total += cnt
        self.assertEquals(total, 100)
        self.assertEquals(self.conn.get(base), '100')

    def test_sharded_search(self):
        _fake_shards_for(self.conn, 'search', 2, 2)
        
        docs = 'hello world how are you doing'.split(), 'this world is doing fine'.split()
        for i in xrange(50):
            c = get_sharded_connection('search', i, 2)
            index_document(c, i, docs[i&1], {'updated':time.time() + i, 'id':i, 'created':time.time() + i})
            r = search_and_sort(c, docs[i&1], sort='-id')
            self.assertEquals(r[1][0], str(i))

        total = 0
        for shard in (0,1):
            count = search_get_values(get_redis_connection('search:%s'%shard),['this', 'world'], num=50)[0]
            total += count
            self.assertTrue(count < 50)
            self.assertTrue(count > 0)
        
        self.assertEquals(total, 25)
        
        count, r, id = get_shard_results('search', 2, ['world', 'doing'], num=50)
        self.assertEquals(count, 50)
        self.assertEquals(count, len(r))
        
        self.assertEquals(get_shard_results('search', 2, ['this', 'doing'], num=50)[0], 25)

        count, r, id = get_shard_results_thread('search', 2, ['this', 'doing'], num=50)
        self.assertEquals(count, 25)
        self.assertEquals(count, len(r))
        r.sort(key=lambda x:x[1], reverse=True)
        r = list(zip(*r)[0])
        
        count, r2, id = search_shards('search', 2, ['this', 'doing'])
        self.assertEquals(count, 25)
        self.assertEquals(len(r2), 20)
        self.assertEquals(r2, r[:20])
        
    def test_sharded_follow_user(self):
        _fake_shards_for(self.conn, 'timelines', 8, 4)

        sharded_timelines['profile:1'].zadd('profile:1', 1, time.time())
        for u2 in xrange(2, 11):
            sharded_timelines['profile:%i'%u2].zadd('profile:%i'%u2, u2, time.time() + u2)
            _follow_user(self.conn, 1, u2)
            _follow_user(self.conn, u2, 1)
        
        self.assertEquals(self.conn.zcard('followers:1'), 9)
        self.assertEquals(self.conn.zcard('following:1'), 9)
        self.assertEquals(sharded_timelines['home:1'].zcard('home:1'), 9)
        
        for db in xrange(14, 10, -1):
            self.assertTrue(len(redis.Redis(db=db).keys()) > 0)
        for u2 in xrange(2, 11):
            self.assertEquals(self.conn.zcard('followers:%i'%u2), 1)
            self.assertEquals(self.conn.zcard('following:%i'%u2), 1)
            self.assertEquals(sharded_timelines['home:%i'%u2].zcard('home:%i'%u2), 1)

    def test_sharded_follow_user_and_syndicate_status(self):
        _fake_shards_for(self.conn, 'timelines', 8, 4)
        _fake_shards_for(self.conn, 'followers', 4, 4)
        sharded_followers.shards = 4
    
        sharded_timelines['profile:1'].zadd('profile:1', 1, time.time())
        for u2 in xrange(2, 11):
            sharded_timelines['profile:%i'%u2].zadd('profile:%i'%u2, u2, time.time() + u2)
            follow_user(self.conn, 1, u2)
            follow_user(self.conn, u2, 1)
        
        allkeys = defaultdict(int)
        for db in xrange(14, 10, -1):
            c = redis.Redis(db=db)
            for k in c.keys():
                allkeys[k] += c.zcard(k)

        for k, v in allkeys.iteritems():
            part, _, owner = k.partition(':')
            if part in ('following', 'followers', 'home'):
                self.assertEquals(v, 9 if owner == '1' else 1)
            elif part == 'profile':
                self.assertEquals(v, 1)

        self.assertEquals(len(sharded_zrangebyscore('followers', 4, 'followers:1', '0', 'inf', 100)), 9)
        syndicate_status(1, {'11':time.time()})
        self.assertEquals(len(sharded_zrangebyscore('timelines', 4, 'home:2', '0', 'inf', 100)), 2)



if __name__ == '__main__':
    unittest.main()
