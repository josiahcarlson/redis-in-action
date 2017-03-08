
import bisect
import math
import threading
import time
import unittest
import uuid

import redis

# <start id="script-load"/>
def script_load(script):
    sha = [None]                #A
    def call(conn, keys=[], args=[], force_eval=False):   #B
        if not force_eval:
            if not sha[0]:   #C
                sha[0] = conn.execute_command(              #D
                    "SCRIPT", "LOAD", script, parse="LOAD") #D
    
            try:
                return conn.execute_command(                    #E
                    "EVALSHA", sha[0], len(keys), *(keys+args)) #E
        
            except redis.exceptions.ResponseError as msg:
                if not msg.args[0].startswith("NOSCRIPT"):      #F
                    raise                                       #F
        
        return conn.execute_command(                    #G
            "EVAL", script, len(keys), *(keys+args))    #G
    
    return call             #H
# <end id="script-load"/>
#A Store the cached SHA1 hash of the result of SCRIPT LOAD in a list so we can change it later from within the call() function
#B When calling the "loaded script", you must provide the connection, the set of keys that the script will manipulate, and any other arguments to the function
#C We will only try loading the script if we don't already have a cached SHA1 hash
#D Load the script if we don't already have the SHA1 hash cached
#E Execute the command from the cached SHA1
#F If the error was unrelated to a missing script, re-raise the exception
#G If we received a script-related error, or if we need to force-execute the script, directly execute the script, which will automatically cache the script on the server (with the same SHA1 that we've already cached) when done
#H Return the function that automatically loads and executes scripts when called
#END

'''
# <start id="show-script-load"/>
>>> ret_1 = script_load("return 1")     #A
>>> ret_1(conn)                         #B
1L                                      #C
# <end id="show-script-load"/>
#A Most uses will load the script and store a reference to the returned function
#B You can then call the function by passing the connection object and any desired arguments
#C Results will be returned and converted into appropriate Python types, when possible
#END
'''


# <start id="ch08-post-status"/>
def create_status(conn, uid, message, **data):
    pipeline = conn.pipeline(True)
    pipeline.hget('user:%s' % uid, 'login') #A
    pipeline.incr('status:id:')             #B
    login, id = pipeline.execute()

    if not login:                           #C
        return None                         #C

    data.update({
        'message': message,                 #D
        'posted': time.time(),              #D
        'id': id,                           #D
        'uid': uid,                         #D
        'login': login,                     #D
    })
    pipeline.hmset('status:%s' % id, data)  #D
    pipeline.hincrby('user:%s' % uid, 'posts')#E
    pipeline.execute()
    return id                               #F
# <end id="ch08-post-status"/>
#A Get the user's login name from their user id
#B Create a new id for the status message
#C Verify that we have a proper user account before posting
#D Prepare and set the data for the status message
#E Record the fact that a status message has been posted
#F Return the id of the newly created status message
#END

_create_status = create_status
# <start id="post-status-lua"/>
def create_status(conn, uid, message, **data):          #H
    args = [                                            #I
        'message', message,                             #I
        'posted', time.time(),                          #I
        'uid', uid,                                     #I
    ]
    for key, value in data.iteritems():                 #I
        args.append(key)                                #I
        args.append(value)                              #I

    return create_status_lua(                           #J
        conn, ['user:%s' % uid, 'status:id:'], args)    #J

create_status_lua = script_load('''
local login = redis.call('hget', KEYS[1], 'login')      --A
if not login then                                       --B
    return false                                        --B
end
local id = redis.call('incr', KEYS[2])                  --C
local key = string.format('status:%s', id)              --D

redis.call('hmset', key,                                --E
    'login', login,                                     --E
    'id', id,                                           --E
    unpack(ARGV))                                       --E
redis.call('hincrby', KEYS[1], 'posts', 1)              --F

return id                                               --G
''')
# <end id="post-status-lua"/>
#A Fetch the user's login name from their id, remember that tables in Lua are 1-indexed, not 0-indexed like Python and most other languages
#B If there is no login, return that no login was found
#C Get a new id for the status message
#D Prepare the destination key for the status message
#E Set the data for the status message
#F Increment the post count of the user
#G Return the id of the status message
#H Take all of the arguments as before
#I Prepare the arguments/attributes to be set on the status message
#J Call the script
#END

# <start id="old-lock"/>
def acquire_lock_with_timeout(
    conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())                      #A
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))         #D
    
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):            #B
            conn.expire(lockname, lock_timeout)         #B
            return identifier
        elif conn.ttl(lockname) < 0:                    #C
            conn.expire(lockname, lock_timeout)         #C
    
        time.sleep(.001)
    
    return False
# <end id="old-lock"/>
#A A 128-bit random identifier
#B Get the lock and set the expiration
#C Check and update the expiration time as necessary
#D Only pass integers to our EXPIRE calls
#END

_acquire_lock_with_timeout = acquire_lock_with_timeout

# <start id="lock-in-lua"/>
def acquire_lock_with_timeout(
    conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())                      
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))      
    
    acquired = False
    end = time.time() + acquire_timeout
    while time.time() < end and not acquired:
        acquired = acquire_lock_with_timeout_lua(                   #A
            conn, [lockname], [lock_timeout, identifier]) == 'OK'   #A
    
        time.sleep(.001 * (not acquired))
    
    return acquired and identifier

acquire_lock_with_timeout_lua = script_load('''
if redis.call('exists', KEYS[1]) == 0 then              --B
    return redis.call('setex', KEYS[1], unpack(ARGV))   --C
end
''')
# <end id="lock-in-lua"/>
#A Actually acquire the lock, checking to verify that the Lua call completed successfully
#B If the lock doesn't already exist, again remembering that tables use 1-based indexing
#C Set the key with the provided expiration and identifier
#END

def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    
    while True:
        try:
            pipe.watch(lockname)                  #A
            if pipe.get(lockname) == identifier:  #A
                pipe.multi()                      #B
                pipe.delete(lockname)             #B
                pipe.execute()                    #B
                return True                       #B
    
            pipe.unwatch()
            break
    
        except redis.exceptions.WatchError:       #C
            pass                                  #C
    
    return False                                  #D

_release_lock = release_lock

# <start id="release-lock-in-lua"/>
def release_lock(conn, lockname, identifier):
    lockname = 'lock:' + lockname
    return release_lock_lua(conn, [lockname], [identifier]) #A

release_lock_lua = script_load('''
if redis.call('get', KEYS[1]) == ARGV[1] then               --B
    return redis.call('del', KEYS[1]) or true               --C
end
''')
# <end id="release-lock-in-lua"/>
#A Call the Lua function that releases the lock
#B Make sure that the lock matches
#C Delete the lock and ensure that we return true
#END

# <start id="old-acquire-semaphore"/>
def acquire_semaphore(conn, semname, limit, timeout=10):
    identifier = str(uuid.uuid4())                             #A
    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)  #B
    pipeline.zadd(semname, identifier, now)                    #C
    pipeline.zrank(semname, identifier)                        #D
    if pipeline.execute()[-1] < limit:                         #D
        return identifier

    conn.zrem(semname, identifier)                             #E
    return None
# <end id="old-acquire-semaphore"/>
#A A 128-bit random identifier
#B Time out old semaphore holders
#C Try to acquire the semaphore
#D Check to see if we have it
#E We failed to get the semaphore, discard our identifier
#END

_acquire_semaphore = acquire_semaphore

# <start id="acquire-semaphore-lua"/>
def acquire_semaphore(conn, semname, limit, timeout=10):
    now = time.time()                                           #A
    return acquire_semaphore_lua(conn, [semname],               #B
        [now-timeout, limit, now, str(uuid.uuid4())])           #B

acquire_semaphore_lua = script_load('''
redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1])        --C

if redis.call('zcard', KEYS[1]) < tonumber(ARGV[2]) then        --D
    redis.call('zadd', KEYS[1], ARGV[3], ARGV[4])               --E
    return ARGV[4]
end
''')
# <end id="acquire-semaphore-lua"/>
#A Get the current timestamp for handling timeouts
#B Pass all of the required arguments into the Lua function to actually acquire the semaphore
#C Clean out all of the expired semaphores
#D If we have not yet hit our semaphore limit, then acquire the semaphore
#E Add the timestamp the timeout ZSET
#END

def release_semaphore(conn, semname, identifier):
    return conn.zrem(semname, identifier)

# <start id="refresh-semaphore-lua"/>
def refresh_semaphore(conn, semname, identifier):
    return refresh_semaphore_lua(conn, [semname],
        [identifier, time.time()]) != None          #A

refresh_semaphore_lua = script_load('''
if redis.call('zscore', KEYS[1], ARGV[1]) then                   --B
    return redis.call('zadd', KEYS[1], ARGV[2], ARGV[1]) or true --B
end
''')
# <end id="refresh-semaphore-lua"/>
#A If Lua had returned "nil" from the call (the semaphore wasn't refreshed), Python will return None instead
#B If the semaphore is still valid, then we update the semaphore's timestamp
#END

valid_characters = '`abcdefghijklmnopqrstuvwxyz{'             #A

def find_prefix_range(prefix):
    posn = bisect.bisect_left(valid_characters, prefix[-1:])  #B
    suffix = valid_characters[(posn or 1) - 1]                #C
    return prefix[:-1] + suffix + '{', prefix + '{'           #D

# <start id="old-autocomplete-code"/>
def autocomplete_on_prefix(conn, guild, prefix):
    start, end = find_prefix_range(prefix)                 #A
    identifier = str(uuid.uuid4())                         #A
    start += identifier                                    #A
    end += identifier                                      #A
    zset_name = 'members:' + guild

    conn.zadd(zset_name, start, 0, end, 0)                 #B
    pipeline = conn.pipeline(True)
    while 1:
        try:
            pipeline.watch(zset_name)
            sindex = pipeline.zrank(zset_name, start)      #C
            eindex = pipeline.zrank(zset_name, end)        #C
            erange = min(sindex + 9, eindex - 2)           #C
            pipeline.multi()
            pipeline.zrem(zset_name, start, end)           #D
            pipeline.zrange(zset_name, sindex, erange)     #D
            items = pipeline.execute()[-1]                 #D
            break
        except redis.exceptions.WatchError:                #E
            continue                                       #E

    return [item for item in items if '{' not in item]     #F
# <end id="old-autocomplete-code"/>
#A Find the start/end range for the prefix
#B Add the start/end range items to the ZSET
#C Find the ranks of our end points
#D Get the values inside our range, and clean up
#E Retry if someone modified our autocomplete zset
#F Remove start/end entries if an autocomplete was in progress
#END

_autocomplete_on_prefix = autocomplete_on_prefix
# <start id="autocomplete-on-prefix-lua"/>
def autocomplete_on_prefix(conn, guild, prefix):
    start, end = find_prefix_range(prefix)                  #A
    identifier = str(uuid.uuid4())                          #A
    
    items = autocomplete_on_prefix_lua(conn,                #B
        ['members:' + guild],                               #B
        [start+identifier, end+identifier])                 #B
    
    return [item for item in items if '{' not in item]      #C

autocomplete_on_prefix_lua = script_load('''
redis.call('zadd', KEYS[1], 0, ARGV[1], 0, ARGV[2])             --D
local sindex = redis.call('zrank', KEYS[1], ARGV[1])            --E
local eindex = redis.call('zrank', KEYS[1], ARGV[2])            --E
eindex = math.min(sindex + 9, eindex - 2)                       --F

redis.call('zrem', KEYS[1], unpack(ARGV))                       --G
return redis.call('zrange', KEYS[1], sindex, eindex)            --H
''')
# <end id="autocomplete-on-prefix-lua"/>
#A Get the range and identifier
#B Fetch the data from Redis with the Lua script
#C Filter out any items that we don't want
#D Add our place-holder endpoints to the ZSET
#E Find the endpoint positions in the ZSET
#F Calculate the proper range of values to fetch
#G Remove the place-holder endpoints
#H Fetch and return our results
#END

# <start id="ch06-purchase-item-with-lock"/>
def purchase_item_with_lock(conn, buyerid, itemid, sellerid):
    buyer = "users:%s" % buyerid
    seller = "users:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    inventory = "inventory:%s" % buyerid

    locked = acquire_lock(conn, 'market:')     #A
    if not locked:
        return False

    pipe = conn.pipeline(True)
    try:
        pipe.zscore("market:", item)           #B
        pipe.hget(buyer, 'funds')              #B
        price, funds = pipe.execute()          #B
        if price is None or price > funds:     #B
            return None                        #B

        pipe.hincrby(seller, 'funds', int(price))  #C
        pipe.hincrby(buyer, 'funds', int(-price))  #C
        pipe.sadd(inventory, itemid)               #C
        pipe.zrem("market:", item)                 #C
        pipe.execute()                             #C
        return True
    finally:
        release_lock(conn, 'market:', locked)      #D
# <end id="ch06-purchase-item-with-lock"/>
#A Get the lock
#B Check for a sold item or insufficient funds
#C Transfer funds from the buyer to the seller, and transfer the item to the buyer
#D Release the lock
#END

# <start id="purchase-item-lua"/>
def purchase_item(conn, buyerid, itemid, sellerid):
    buyer = "users:%s" % buyerid                        #A
    seller = "users:%s" % sellerid                      #A
    item = "%s.%s"%(itemid, sellerid)                   #A
    inventory = "inventory:%s" % buyerid                #A

    return purchase_item_lua(conn,
        ['market:', buyer, seller, inventory], [item, itemid])

purchase_item_lua = script_load('''
local price = tonumber(redis.call('zscore', KEYS[1], ARGV[1]))  --B
local funds = tonumber(redis.call('hget', KEYS[2], 'funds'))    --B

if price and funds and funds >= price then                      --C
    redis.call('hincrby', KEYS[3], 'funds', price)              --C
    redis.call('hincrby', KEYS[2], 'funds', -price)             --C
    redis.call('sadd', KEYS[4], ARGV[2])                        --C
    redis.call('zrem', KEYS[1], ARGV[1])                        --C
    return true                                                 --D
end
''')
# <end id="purchase-item-lua"/>
#A Prepare all of the keys and arguments for the Lua script
#B Get the item price and the buyer's available funds
#C If the item is still available and the buyer has enough money, transfer the item
#D Signify that the purchase completed successfully
#END

def list_item(conn, itemid, sellerid, price):
    inv = "inventory:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    return list_item_lua(conn, [inv, 'market:'], [itemid, item, price])

list_item_lua = script_load('''
if redis.call('sismember', KEYS[1], ARGV[1]) ~= 0 then
    redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])
    redis.call('srem', KEYS[1], ARGV[1])
    return true
end
''')

# <start id="sharded-list-push"/>
def sharded_push_helper(conn, key, *items, **kwargs):
    items = list(items)                                 #A
    total = 0
    while items:                                        #B
        pushed = sharded_push_lua(conn,                 #C
            [key+':', key+':first', key+':last'],       #C
            [kwargs['cmd']] + items[:64])               #D
        total += pushed                                 #E
        del items[:pushed]                              #F
    return total                                        #G

def sharded_lpush(conn, key, *items):
    return sharded_push_helper(conn, key, *items, cmd='lpush')#H

def sharded_rpush(conn, key, *items):
    return sharded_push_helper(conn, key, *items, cmd='rpush')#H

sharded_push_lua = script_load('''
local max = tonumber(redis.call(                            --I
    'config', 'get', 'list-max-ziplist-entries')[2])        --I
if #ARGV < 2 or max < 2 then return 0 end                   --J

local skey = ARGV[1] == 'lpush' and KEYS[2] or KEYS[3]      --K
local shard = redis.call('get', skey) or '0'                --K

while 1 do
    local current = tonumber(redis.call('llen', KEYS[1]..shard))    --L
    local topush = math.min(#ARGV - 1, max - current - 1)           --M
    if topush > 0 then                                              --N
        redis.call(ARGV[1], KEYS[1]..shard, unpack(ARGV, 2, topush+1))--N
        return topush                                                 --N
    end
    shard = redis.call(ARGV[1] == 'lpush' and 'decr' or 'incr', skey) --O
end
''')
# <end id="sharded-list-push"/>
#A Convert our sequence of items into a list
#B While we still have items to push
#C Push items onto the sharded list by calling the Lua script
#D Note that we only push up to 64 items at a time here, you may want to adjust this up or down, depending on your maximum list ziplist size
#E Count the number of items that we pushed
#F Remove the items that we've already pushed
#G Return the total number of items pushed
#H Make a call to the sharded_push_helper function with a special argument that tells it to use lpush or rpush
#I Determine the maximum size of a LIST shard
#J If there is nothing to push, or if our max ziplist LIST entries is too small, return 0
#K Find out whether we are pushing onto the left or right end of the LIST, and get the correct end shard
#L Get the current length of that shard
#M Calculate how many of our current number of items we can push onto the current LIST shard without going over the limit, saving one entry for later blocking pop purposes
#N If we can push some items, then push as many items as we can
#O Otherwise generate a new shard, and try again
#END

def sharded_llen(conn, key):
    return sharded_llen_lua(conn, [key+':', key+':first', key+':last'])

sharded_llen_lua = script_load('''
local shardsize = tonumber(redis.call(
    'config', 'get', 'list-max-ziplist-entries')[2])

local first = tonumber(redis.call('get', KEYS[2]) or '0')
local last = tonumber(redis.call('get', KEYS[3]) or '0')

local total = 0
total = total + tonumber(redis.call('llen', KEYS[1]..first))
if first ~= last then
    total = total + (last - first - 1) * (shardsize-1)
    total = total + tonumber(redis.call('llen', KEYS[1]..last))
end

return total
''')

# <start id="sharded-list-pop-lua"/>
def sharded_lpop(conn, key):
    return sharded_list_pop_lua(
        conn, [key+':', key+':first', key+':last'], ['lpop'])

def sharded_rpop(conn, key):
    return sharded_list_pop_lua(
        conn, [key+':', key+':first', key+':last'], ['rpop'])

sharded_list_pop_lua = script_load('''
local skey = ARGV[1] == 'lpop' and KEYS[2] or KEYS[3]           --A
local okey = ARGV[1] ~= 'lpop' and KEYS[2] or KEYS[3]           --B
local shard = redis.call('get', skey) or '0'                    --C

local ret = redis.call(ARGV[1], KEYS[1]..shard)                 --D
if not ret or redis.call('llen', KEYS[1]..shard) == '0' then    --E
    local oshard = redis.call('get', okey) or '0'               --F

    if shard == oshard then                                     --G
        return ret                                              --G
    end

    local cmd = ARGV[1] == 'lpop' and 'incr' or 'decr'          --H
    shard = redis.call(cmd, skey)                               --I
    if not ret then
        ret = redis.call(ARGV[1], KEYS[1]..shard)               --J
    end
end
return ret
''')
# <end id="sharded-list-pop-lua"/>
#A Get the key for the end we will be popping from
#B Get the key for the end we won't be popping from
#C Get the shard id that we will be popping from
#D Pop from the shard
#E If we didn't get anything because the shard was empty, or we have just made the shard empty, we should clean up our shard endpoint
#F Get the shard id for the end we didn't pop from
#G If both ends of the sharded LIST are the same, then the list is now empty and we are done
#H Determine whether to increment or decrement the shard id, based on whether we were popping off the left or right end
#I Adjust our shard endpoint
#J If we didn't get a value before, try again on the new shard
#END

# <start id="sharded-blocking-list-pop"/>
DUMMY = str(uuid.uuid4())                                           #A

def sharded_bpop_helper(conn, key, timeout, pop, bpop, endp, push): #B
    pipe = conn.pipeline(False)                                     #C
    timeout = max(timeout, 0) or 2**64                              #C
    end = time.time() + timeout                                     #C
    
    while time.time() < end:
        result = pop(conn, key)                                     #D
        if result not in (None, DUMMY):                             #D
            return result                                           #D
    
        shard = conn.get(key + endp) or '0'                         #E
        sharded_bpop_helper_lua(pipe, [key + ':', key + endp],      #F
            [shard, push, DUMMY], force_eval=True)                  #L
        getattr(pipe, bpop)(key + ':' + shard, 1)                   #G
    
        result = (pipe.execute()[-1] or [None])[-1]                 #H
        if result not in (None, DUMMY):                             #H
            return result                                           #H

def sharded_blpop(conn, key, timeout=0):                              #I
    return sharded_bpop_helper(                                       #I
        conn, key, timeout, sharded_lpop, 'blpop', ':first', 'lpush') #I

def sharded_brpop(conn, key, timeout=0):                              #I
    return sharded_bpop_helper(                                       #I
        conn, key, timeout, sharded_rpop, 'brpop', ':last', 'rpush')  #I

sharded_bpop_helper_lua = script_load('''
local shard = redis.call('get', KEYS[2]) or '0'                     --J
if shard ~= ARGV[1] then                                            --K
    redis.call(ARGV[2], KEYS[1]..ARGV[1], ARGV[3])                  --K
end
''')
# <end id="sharded-blocking-list-pop"/>
#A Our defined dummy value, which you can change to be something that you shouldn't expect to see in your sharded LISTs
#B We are going to define a helper function that will actually perform the pop operations for both types of blocking pop operations
#C Prepare the pipeline and timeout information
#D Try to perform a non-blocking pop, returning the value it it isn't missing or the dummy value
#E Get the shard that we think we need to pop from
#F Run the Lua helper, which will handle pushing a dummy value if we are popping from the wrong shard
#L We use force_eval here to ensure an EVAL call instead of an EVALSHA, because we can't afford to perform a potentially failing EVALSHA inside a pipeline
#G Try to block on popping the item from the LIST, using the proper 'blpop' or 'brpop' command passed in
#H If we got an item, then we are done, otherwise retry
#I These functions prepare the actual call to the underlying blocking pop operations
#J Get the actual shard for the end we want to pop from
#K If we were going to try to pop from the wrong shard, push an extra value
#END

class TestCh11(unittest.TestCase):
    def setUp(self):
        self.conn = redis.Redis(db=15)
        self.conn.flushdb()
    def tearDown(self):
        self.conn.flushdb()

    def test_load_script(self):
        self.assertEquals(script_load("return 1")(self.conn), 1)

    def test_create_status(self):
        self.conn.hset('user:1', 'login', 'test')
        sid = _create_status(self.conn, 1, 'hello')
        sid2 = create_status(self.conn, 1, 'hello')
        
        self.assertEquals(self.conn.hget('user:1', 'posts'), '2')
        data = self.conn.hgetall('status:%s'%sid)
        data2 = self.conn.hgetall('status:%s'%sid2)
        data.pop('posted'); data.pop('id')
        data2.pop('posted'); data2.pop('id')
        self.assertEquals(data, data2)

    def test_locking(self):
        identifier = acquire_lock_with_timeout(self.conn, 'test', 1, 5)
        self.assertTrue(identifier)
        self.assertFalse(acquire_lock_with_timeout(self.conn, 'test', 1, 5))
        release_lock(self.conn, 'test', identifier)
        self.assertTrue(acquire_lock_with_timeout(self.conn, 'test', 1, 5))
    
    def test_semaphore(self):
        ids = []
        for i in xrange(5):
            ids.append(acquire_semaphore(self.conn, 'test', 5, timeout=1))
        self.assertTrue(None not in ids)
        self.assertFalse(acquire_semaphore(self.conn, 'test', 5, timeout=1))
        time.sleep(.01)
        id = acquire_semaphore(self.conn, 'test', 5, timeout=0)
        self.assertTrue(id)
        self.assertFalse(refresh_semaphore(self.conn, 'test', ids[-1]))
        self.assertFalse(release_semaphore(self.conn, 'test', ids[-1]))

        self.assertTrue(refresh_semaphore(self.conn, 'test', id))
        self.assertTrue(release_semaphore(self.conn, 'test', id))
        self.assertFalse(release_semaphore(self.conn, 'test', id))

    def test_autocomplet_on_prefix(self):
        for word in 'these are some words that we will be autocompleting on'.split():
            self.conn.zadd('members:test', word, 0)
        
        self.assertEquals(autocomplete_on_prefix(self.conn, 'test', 'th'), ['that', 'these'])
        self.assertEquals(autocomplete_on_prefix(self.conn, 'test', 'w'), ['we', 'will', 'words'])
        self.assertEquals(autocomplete_on_prefix(self.conn, 'test', 'autocompleting'), ['autocompleting'])

    def test_marketplace(self):
        self.conn.sadd('inventory:1', '1')
        self.conn.hset('users:2', 'funds', 5)
        self.assertFalse(list_item(self.conn, 2, 1, 10))
        self.assertTrue(list_item(self.conn, 1, 1, 10))
        self.assertFalse(purchase_item(self.conn, 2, '1', 1))
        self.conn.zadd('market:', '1.1', 4)
        self.assertTrue(purchase_item(self.conn, 2, '1', 1))

    def test_sharded_list(self):
        self.assertEquals(sharded_lpush(self.conn, 'lst', *range(100)), 100)
        self.assertEquals(sharded_llen(self.conn, 'lst'), 100)

        self.assertEquals(sharded_lpush(self.conn, 'lst2', *range(1000)), 1000)
        self.assertEquals(sharded_llen(self.conn, 'lst2'), 1000)
        self.assertEquals(sharded_rpush(self.conn, 'lst2', *range(-1, -1001, -1)), 1000)
        self.assertEquals(sharded_llen(self.conn, 'lst2'), 2000)

        self.assertEquals(sharded_lpop(self.conn, 'lst2'), '999')
        self.assertEquals(sharded_rpop(self.conn, 'lst2'), '-1000')
        
        for i in xrange(999):
            r = sharded_lpop(self.conn, 'lst2')
        self.assertEquals(r, '0')

        results = []
        def pop_some(conn, fcn, lst, count, timeout):
            for i in xrange(count):
                results.append(sharded_blpop(conn, lst, timeout))
        
        t = threading.Thread(target=pop_some, args=(self.conn, sharded_blpop, 'lst3', 10, 1))
        t.setDaemon(1)
        t.start()
        
        self.assertEquals(sharded_rpush(self.conn, 'lst3', *range(4)), 4)
        time.sleep(2)
        self.assertEquals(sharded_rpush(self.conn, 'lst3', *range(4, 8)), 4)
        time.sleep(2)
        self.assertEquals(results, ['0', '1', '2', '3', None, '4', '5', '6', '7', None])

if __name__ == '__main__':
    unittest.main()
