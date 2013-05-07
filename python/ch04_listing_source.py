
import os
import time
import unittest
import uuid

import redis

'''
# <start id="persistence-options"/>
save 60 1000                        #A
stop-writes-on-bgsave-error no      #A
rdbcompression yes                  #A
dbfilename dump.rdb                 #A

appendonly no                       #B
appendfsync everysec                #B
no-appendfsync-on-rewrite no        #B
auto-aof-rewrite-percentage 100     #B
auto-aof-rewrite-min-size 64mb      #B

dir ./                              #C
# <end id="persistence-options"/>
#A Snapshotting persistence options
#B Append-only file persistence options
#C Shared option, where to store the snapshot or append-only file
#END
'''

# <start id="process-logs-progress"/>
def process_logs(conn, path, callback):                     #K
    current_file, offset = conn.mget(                       #A
        'progress:file', 'progress:position')               #A

    pipe = conn.pipeline()

    def update_progress():                                  #H
        pipe.mset({                                         #I
            'progress:file': fname,                         #I
            'progress:position': offset                     #I
        })
        pipe.execute()                                      #J

    for fname in sorted(os.listdir(path)):                  #B
        if fname < current_file:                            #C
            continue

        inp = open(os.path.join(path, fname), 'rb')
        if fname == current_file:                           #D
            inp.seek(int(offset, 10))                       #D
        else:
            offset = 0

        current_file = None

        for lno, line in enumerate(inp):                    #L
            callback(pipe, line)                            #E
            offset += int(offset) + len(line)               #F

            if not (lno+1) % 1000:                          #G
                update_progress()                           #G
        update_progress()                                   #G

        inp.close()
# <end id="process-logs-progress"/>
#A Get the current progress
#B Iterate over the logfiles in sorted order
#C Skip over files that are before the current file
#D If we are continuing a file, skip over the parts that we've already processed
#E Handle the log line
#F Update our information about the offset into the file
#G Write our progress back to Redis every 1000 lines, or when we are done with a file
#H This closure is meant primarily to reduce the number of duplicated lines later
#I We want to update our file and line number offsets into the logfile
#J This will execute any outstanding log updates, as well as to actually write our file and line number updates to Redis
#K Our function will be provided with a callback that will take a connection and a log line, calling methods on the pipeline as necessary
#L The enumerate function iterates over a sequence (in this case lines from a file), and produces pairs consisting of a numeric sequence starting from 0, and the original data
#END

# <start id="wait-for-sync"/>
def wait_for_sync(mconn, sconn):
    identifier = str(uuid.uuid4())
    mconn.zadd('sync:wait', identifier, time.time())        #A

    while not sconn.info()['master_link_status'] != 'up':   #B
        time.sleep(.001)

    while not sconn.zscore('sync:wait', identifier):        #C
        time.sleep(.001)

    deadline = time.time() + 1.01                           #D
    while time.time() < deadline:                           #D
        if sconn.info()['aof_pending_bio_fsync'] == 0:      #E
            break                                           #E
        time.sleep(.001)

    mconn.zrem('sync:wait', identifier)                     #F
    mconn.zremrangebyscore('sync:wait', 0, time.time()-900) #F
# <end id="wait-for-sync"/>
#A Add the token to the master
#B Wait for the slave to sync (if necessary)
#C Wait for the slave to receive the data change
#D Wait up to 1 second
#E Check to see if the data is known to be on disk
#F Clean up our status and clean out older entries that may have been left there
#END

'''
# <start id="master-failover"/>
user@vpn-master ~:$ ssh root@machine-b.vpn                          #A
Last login: Wed Mar 28 15:21:06 2012 from ...                       #A
root@machine-b ~:$ redis-cli                                        #B
redis 127.0.0.1:6379> SAVE                                          #C
OK                                                                  #C
redis 127.0.0.1:6379> QUIT                                          #C
root@machine-b ~:$ scp \\                                           #D
> /var/local/redis/dump.rdb machine-c.vpn:/var/local/redis/         #D
dump.rdb                      100%   525MB  8.1MB/s   01:05         #D
root@machine-b ~:$ ssh machine-c.vpn                                #E
Last login: Tue Mar 27 12:42:31 2012 from ...                       #E
root@machine-c ~:$ sudo /etc/init.d/redis-server start              #E
Starting Redis server...                                            #E
root@machine-c ~:$ exit
root@machine-b ~:$ redis-cli                                        #F
redis 127.0.0.1:6379> SLAVEOF machine-c.vpn 6379                    #F
OK                                                                  #F
redis 127.0.0.1:6379> QUIT
root@machine-b ~:$ exit
user@vpn-master ~:$
# <end id="master-failover"/>
#A Connect to machine B on our vpn network
#B Start up the command line redis client to do a few simple operations
#C Start a SAVE, and when it is done, QUIT so that we can continue
#D Copy the snapshot over to the new master, machine C
#E Connect to the new master and start Redis
#F Tell machine B's Redis that it should use C as the new master
#END
'''

# <start id="_1313_14472_8342"/>
def list_item(conn, itemid, sellerid, price):
    inventory = "inventory:%s"%sellerid
    item = "%s.%s"%(itemid, sellerid)
    end = time.time() + 5
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            pipe.watch(inventory)                    #A
            if not pipe.sismember(inventory, itemid):#B
                pipe.unwatch()                       #E
                return None

            pipe.multi()                             #C
            pipe.zadd("market:", item, price)        #C
            pipe.srem(inventory, itemid)             #C
            pipe.execute()                           #F
            return True
        except redis.exceptions.WatchError:          #D
            pass                                     #D
    return False
# <end id="_1313_14472_8342"/>
#A Watch for changes to the users's inventory
#B Verify that the user still has the item to be listed
#E If the item is not in the user's inventory, stop watching the inventory key and return
#C Actually list the item
#F If execute returns without a WatchError being raised, then the transaction is complete and the inventory key is no longer watched
#D The user's inventory was changed, retry
#END

# <start id="_1313_14472_8353"/>
def purchase_item(conn, buyerid, itemid, sellerid, lprice):
    buyer = "users:%s"%buyerid
    seller = "users:%s"%sellerid
    item = "%s.%s"%(itemid, sellerid)
    inventory = "inventory:%s"%buyerid
    end = time.time() + 10
    pipe = conn.pipeline()

    while time.time() < end:
        try:
            pipe.watch("market:", buyer)                #A

            price = pipe.zscore("market:", item)        #B
            funds = int(pipe.hget(buyer, "funds"))      #B
            if price != lprice or price > funds:        #B
                pipe.unwatch()                          #B
                return None

            pipe.multi()                                #C
            pipe.hincrby(seller, "funds", int(price))   #C
            pipe.hincrby(buyer, "funds", int(-price))   #C
            pipe.sadd(inventory, itemid)                #C
            pipe.zrem("market:", item)                  #C
            pipe.execute()                              #C
            return True
        except redis.exceptions.WatchError:             #D
            pass                                        #D

    return False
# <end id="_1313_14472_8353"/>
#A Watch for changes to the market and to the buyer's account information
#B Check for a sold/repriced item or insufficient funds
#C Transfer funds from the buyer to the seller, and transfer the item to the buyer
#D Retry if the buyer's account or the market changed
#END


# <start id="update-token"/>
def update_token(conn, token, user, item=None):
    timestamp = time.time()                             #A
    conn.hset('login:', token, user)                    #B
    conn.zadd('recent:', token, timestamp)              #C
    if item:
        conn.zadd('viewed:' + token, item, timestamp)   #D
        conn.zremrangebyrank('viewed:' + token, 0, -26) #E
        conn.zincrby('viewed:', item, -1)               #F
# <end id="update-token"/>
#A Get the timestamp
#B Keep a mapping from the token to the logged-in user
#C Record when the token was last seen
#D Record that the user viewed the item
#E Remove old items, keeping the most recent 25
#F Update the number of times the given item had been viewed
#END

# <start id="update-token-pipeline"/>
def update_token_pipeline(conn, token, user, item=None):
    timestamp = time.time()
    pipe = conn.pipeline(False)                         #A
    pipe.hset('login:', token, user)
    pipe.zadd('recent:', token, timestamp)
    if item:
        pipe.zadd('viewed:' + token, item, timestamp)
        pipe.zremrangebyrank('viewed:' + token, 0, -26)
        pipe.zincrby('viewed:', item, -1)
    pipe.execute()                                      #B
# <end id="update-token-pipeline"/>
#A Set up the pipeline
#B Execute the commands in the pipeline
#END

# <start id="simple-pipeline-benchmark-code"/>
def benchmark_update_token(conn, duration):
    for function in (update_token, update_token_pipeline):      #A
        count = 0                                               #B
        start = time.time()                                     #B
        end = start + duration                                  #B
        while time.time() < end:
            count += 1
            function(conn, 'token', 'user', 'item')             #C
        delta = time.time() - start                             #D
        print function.__name__, count, delta, count / delta    #E
# <end id="simple-pipeline-benchmark-code"/>
#A Execute both the update_token() and the update_token_pipeline() functions
#B Set up our counters and our ending conditions
#C Call one of the two functions
#D Calculate the duration
#E Print information about the results
#END

'''
# <start id="redis-benchmark"/>
$ redis-benchmark  -c 1 -q                               #A
PING (inline): 34246.57 requests per second
PING: 34843.21 requests per second
MSET (10 keys): 24213.08 requests per second
SET: 32467.53 requests per second
GET: 33112.59 requests per second
INCR: 32679.74 requests per second
LPUSH: 33333.33 requests per second
LPOP: 33670.04 requests per second
SADD: 33222.59 requests per second
SPOP: 34482.76 requests per second
LPUSH (again, in order to bench LRANGE): 33222.59 requests per second
LRANGE (first 100 elements): 22988.51 requests per second
LRANGE (first 300 elements): 13888.89 requests per second
LRANGE (first 450 elements): 11061.95 requests per second
LRANGE (first 600 elements): 9041.59 requests per second
# <end id="redis-benchmark"/>
#A We run with the '-q' option to get simple output, and '-c 1' to use a single client
#END
'''

#--------------- Below this line are helpers to test the code ----------------

class TestCh04(unittest.TestCase):
    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()
        del self.conn
        print
        print

    # We can't test process_logs, as that would require writing to disk, which
    # we don't want to do.

    # We also can't test wait_for_sync, as we can't guarantee that there are
    # multiple Redis servers running with the proper configuration

    def test_list_item(self):
        import pprint
        conn = self.conn

        print "We need to set up just enough state so that a user can list an item"
        seller = 'userX'
        item = 'itemX'
        conn.sadd('inventory:' + seller, item)
        i = conn.smembers('inventory:' + seller)
        print "The user's inventory has:", i
        self.assertTrue(i)
        print

        print "Listing the item..."
        l = list_item(conn, item, seller, 10)
        print "Listing the item succeeded?", l
        self.assertTrue(l)
        r = conn.zrange('market:', 0, -1, withscores=True)
        print "The market contains:"
        pprint.pprint(r)
        self.assertTrue(r)
        self.assertTrue(any(x[0] == 'itemX.userX' for x in r))

    def test_purchase_item(self):
        self.test_list_item()
        conn = self.conn
        
        print "We need to set up just enough state so a user can buy an item"
        buyer = 'userY'
        conn.hset('users:userY', 'funds', 125)
        r = conn.hgetall('users:userY')
        print "The user has some money:", r
        self.assertTrue(r)
        self.assertTrue(r.get('funds'))
        print

        print "Let's purchase an item"
        p = purchase_item(conn, 'userY', 'itemX', 'userX', 10)
        print "Purchasing an item succeeded?", p
        self.assertTrue(p)
        r = conn.hgetall('users:userY')
        print "Their money is now:", r
        self.assertTrue(r)
        i = conn.smembers('inventory:' + buyer)
        print "Their inventory is now:", i
        self.assertTrue(i)
        self.assertTrue('itemX' in i)
        self.assertEquals(conn.zscore('market:', 'itemX.userX'), None)

    def test_benchmark_update_token(self):
        benchmark_update_token(self.conn, 5)

if __name__ == '__main__':
    unittest.main()
