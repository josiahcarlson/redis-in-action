
import bisect
from collections import defaultdict, deque
import json
import math
import os
import time
import unittest
import uuid
import zlib

import redis

QUIT = False
pipe = inv = item = buyer = seller = inventory = None

# <start id="_1314_14473_8380"/>
def add_update_contact(conn, user, contact):
    ac_list = 'recent:' + user
    pipeline = conn.pipeline(True)     #A
    pipeline.lrem(ac_list, contact)    #B
    pipeline.lpush(ac_list, contact)   #C
    pipeline.ltrim(ac_list, 0, 99)     #D
    pipeline.execute()                 #E
# <end id="_1314_14473_8380"/>
#A Set up the atomic operation
#B Remove the contact from the list if it exists
#C Push the item onto the front of the list
#D Remove anything beyond the 100th item
#E Actually execute everything
#END

# <start id="_1314_14473_8383"/>
def remove_contact(conn, user, contact):
    conn.lrem('recent:' + user, contact)
# <end id="_1314_14473_8383"/>
#END

# <start id="_1314_14473_8386"/>
def fetch_autocomplete_list(conn, user, prefix):
    candidates = conn.lrange('recent:' + user, 0, -1) #A
    matches = []
    for candidate in candidates:                      #B
        if candidate.lower().startswith(prefix.lower()):      #B
            matches.append(candidate)                 #C
    return matches                                    #D
# <end id="_1314_14473_8386"/>
#A Fetch the autocomplete list
#B Check each candidate
#C We found a match
#D Return all of the matches
#END

# <start id="_1314_14473_8396"/>
valid_characters = '`abcdefghijklmnopqrstuvwxyz{'             #A

def find_prefix_range(prefix):
    posn = bisect.bisect_left(valid_characters, prefix[-1:])  #B
    suffix = valid_characters[(posn or 1) - 1]                #C
    return prefix[:-1] + suffix + '{', prefix + '{'           #D
# <end id="_1314_14473_8396"/>
#A Set up our list of characters that we know about
#B Find the position of prefix character in our list of characters
#C Find the predecessor character
#D Return the range
#END

# <start id="_1314_14473_8399"/>
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
# <end id="_1314_14473_8399"/>
#A Find the start/end range for the prefix
#B Add the start/end range items to the ZSET
#C Find the ranks of our end points
#D Get the values inside our range, and clean up
#E Retry if someone modified our autocomplete zset
#F Remove start/end entries if an autocomplete was in progress
#END

# <start id="_1314_14473_8403"/>
def join_guild(conn, guild, user):
    conn.zadd('members:' + guild, user, 0)

def leave_guild(conn, guild, user):
    conn.zrem('members:' + guild, user)
# <end id="_1314_14473_8403"/>
#END

# <start id="_1314_14473_8431"/>
def list_item(conn, itemid, sellerid, price):
    #...
            pipe.watch(inv)                             #A
            if not pipe.sismember(inv, itemid):         #B
                pipe.unwatch()                          #B
                return None

            pipe.multi()                                #C
            pipe.zadd("market:", item, price)           #C
            pipe.srem(inv, itemid)                      #C
            pipe.execute()                              #C
            return True
    #...
# <end id="_1314_14473_8431"/>
#A Watch for changes to the users's inventory
#B Verify that the user still has the item to be listed
#C Actually list the item
#END

# <start id="_1314_14473_8435"/>
def purchase_item(conn, buyerid, itemid, sellerid, lprice):
    #...
            pipe.watch("market:", buyer)                #A

            price = pipe.zscore("market:", item)        #B
            funds = int(pipe.hget(buyer, 'funds'))      #B
            if price != lprice or price > funds:        #B
                pipe.unwatch()                          #B
                return None

            pipe.multi()                                #C
            pipe.hincrby(seller, 'funds', int(price))   #C
            pipe.hincrby(buyerid, 'funds', int(-price)) #C
            pipe.sadd(inventory, itemid)                #C
            pipe.zrem("market:", item)                  #C
            pipe.execute()                              #C
            return True

    #...
# <end id="_1314_14473_8435"/>
#A Watch for changes to the market and the buyer's account information
#B Check for a sold/repriced item or insufficient funds
#C Transfer funds from the buyer to the seller, and transfer the item to the buyer
#END

# <start id="_1314_14473_8641"/>
def acquire_lock(conn, lockname, acquire_timeout=10):
    identifier = str(uuid.uuid4())                      #A

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx('lock:' + lockname, identifier):  #B
            return identifier

        time.sleep(.001)

    return False
# <end id="_1314_14473_8641"/>
#A A 128-bit random identifier
#B Get the lock
#END

# <start id="_1314_14473_8645"/>
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
# <end id="_1314_14473_8645"/>
#A Get the lock
#B Check for a sold item or insufficient funds
#C Transfer funds from the buyer to the seller, and transfer the item to the buyer
#D Release the lock
#END

# <start id="_1314_14473_8650"/>
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
# <end id="_1314_14473_8650"/>
#A Check and verify that we still have the lock
#B Release the lock
#C Someone else did something with the lock, retry
#D We lost the lock
#END

# <start id="_1314_14473_8790"/>
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
# <end id="_1314_14473_8790"/>
#A A 128-bit random identifier
#B Get the lock and set the expiration
#C Check and update the expiration time as necessary
#D Only pass integers to our EXPIRE calls
#END

# <start id="_1314_14473_8986"/>
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
# <end id="_1314_14473_8986"/>
#A A 128-bit random identifier
#B Time out old semaphore holders
#C Try to acquire the semaphore
#D Check to see if we have it
#E We failed to get the semaphore, discard our identifier
#END

# <start id="_1314_14473_8990"/>
def release_semaphore(conn, semname, identifier):
    return conn.zrem(semname, identifier)                      #A
# <end id="_1314_14473_8990"/>
#A Returns True if the semaphore was properly released, False if it had timed out
#END

# <start id="_1314_14473_9004"/>
def acquire_fair_semaphore(conn, semname, limit, timeout=10):
    identifier = str(uuid.uuid4())                             #A
    czset = semname + ':owner'
    ctr = semname + ':counter'

    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)  #B
    pipeline.zinterstore(czset, {czset: 1, semname: 0})        #B

    pipeline.incr(ctr)                                         #C
    counter = pipeline.execute()[-1]                           #C

    pipeline.zadd(semname, identifier, now)                    #D
    pipeline.zadd(czset, identifier, counter)                  #D

    pipeline.zrank(czset, identifier)                          #E
    if pipeline.execute()[-1] < limit:                         #E
        return identifier                                      #F

    pipeline.zrem(semname, identifier)                         #G
    pipeline.zrem(czset, identifier)                           #G
    pipeline.execute()
    return None
# <end id="_1314_14473_9004"/>
#A A 128-bit random identifier
#B Time out old entries
#C Get the counter
#D Try to acquire the semaphore
#E Check the rank to determine if we got the semaphore
#F We got the semaphore
#G We didn't get the semaphore, clean out the bad data
#END

# <start id="_1314_14473_9014"/>
def release_fair_semaphore(conn, semname, identifier):
    pipeline = conn.pipeline(True)
    pipeline.zrem(semname, identifier)
    pipeline.zrem(semname + ':owner', identifier)
    return pipeline.execute()[0]                               #A
# <end id="_1314_14473_9014"/>
#A Returns True if the semaphore was properly released, False if it had timed out
#END

# <start id="_1314_14473_9022"/>
def refresh_fair_semaphore(conn, semname, identifier):
    if conn.zadd(semname, identifier, time.time()):            #A
        release_fair_semaphore(conn, semname, identifier)      #B
        return False                                           #B
    return True                                                #C
# <end id="_1314_14473_9022"/>
#A Update our semaphore
#B We lost our semaphore, report back
#C We still have our semaphore
#END

# <start id="_1314_14473_9031"/>
def acquire_semaphore_with_lock(conn, semname, limit, timeout=10):
    identifier = acquire_lock(conn, semname, acquire_timeout=.01)
    if identifier:
        try:
            return acquire_fair_semaphore(conn, semname, limit, timeout)
        finally:
            release_lock(conn, semname, identifier)
# <end id="_1314_14473_9031"/>
#END

# <start id="_1314_14473_9056"/>
def send_sold_email_via_queue(conn, seller, item, price, buyer):
    data = {
        'seller_id': seller,                    #A
        'item_id': item,                        #A
        'price': price,                         #A
        'buyer_id': buyer,                      #A
        'time': time.time()                     #A
    }
    conn.rpush('queue:email', json.dumps(data)) #B
# <end id="_1314_14473_9056"/>
#A Prepare the item
#B Push the item onto the queue
#END

# <start id="_1314_14473_9060"/>
def process_sold_email_queue(conn):
    while not QUIT:
        packed = conn.blpop(['queue:email'], 30)                  #A
        if not packed:                                            #B
            continue                                              #B

        to_send = json.loads(packed[1])                           #C
        try:
            fetch_data_and_send_sold_email(to_send)               #D
        except EmailSendError as err:
            log_error("Failed to send sold email", err, to_send)
        else:
            log_success("Sent sold email", to_send)
# <end id="_1314_14473_9060"/>
#A Try to get a message to send
#B No message to send, try again
#C Load the packed email information
#D Send the email using our pre-written emailing function
#END

# <start id="_1314_14473_9066"/>
def worker_watch_queue(conn, queue, callbacks):
    while not QUIT:
        packed = conn.blpop([queue], 30)                    #A
        if not packed:                                      #B
            continue                                        #B

        name, args = json.loads(packed[1])                  #C
        if name not in callbacks:                           #D
            log_error("Unknown callback %s"%name)           #D
            continue                                        #D
        callbacks[name](*args)                              #E
# <end id="_1314_14473_9066"/>
#A Try to get an item from the queue
#B There is nothing to work on, try again
#C Unpack the work item
#D The function is unknown, log the error and try again
#E Execute the task
#END

# <start id="_1314_14473_9074"/>
def worker_watch_queues(conn, queues, callbacks):   #A
    while not QUIT:
        packed = conn.blpop(queues, 30)             #B
        if not packed:
            continue

        name, args = json.loads(packed[1])
        if name not in callbacks:
            log_error("Unknown callback %s"%name)
            continue
        callbacks[name](*args)
# <end id="_1314_14473_9074"/>
#A The first changed line to add priority support
#B The second changed line to add priority support
#END

# <start id="_1314_14473_9094"/>
def execute_later(conn, queue, name, args, delay=0):
    identifier = str(uuid.uuid4())                          #A
    item = json.dumps([identifier, queue, name, args])      #B
    if delay > 0:
        conn.zadd('delayed:', item, time.time() + delay)    #C
    else:
        conn.rpush('queue:' + queue, item)                  #D
    return identifier                                       #E
# <end id="_1314_14473_9094"/>
#A Generate a unique identifier
#B Prepare the item for the queue
#C Delay the item
#D Execute the item immediately
#E Return the identifier
#END

# <start id="_1314_14473_9099"/>
def poll_queue(conn):
    while not QUIT:
        item = conn.zrange('delayed:', 0, 0, withscores=True)   #A
        if not item or item[0][1] > time.time():                #B
            time.sleep(.01)                                     #B
            continue                                            #B

        item = item[0][0]                                       #C
        identifier, queue, function, args = json.loads(item)    #C

        locked = acquire_lock(conn, identifier)                 #D
        if not locked:                                          #E
            continue                                            #E

        if conn.zrem('delayed:', item):                         #F
            conn.rpush('queue:' + queue, item)                  #F

        release_lock(conn, identifier, locked)                  #G
# <end id="_1314_14473_9099"/>
#A Get the first item in the queue
#B No item or the item is still to be execued in the future
#C Unpack the item so that we know where it should go
#D Get the lock for the item
#E We couldn't get the lock, so skip it and try again
#F Move the item to the proper list queue
#G Release the lock
#END

# <start id="_1314_14473_9124"/>
def create_chat(conn, sender, recipients, message, chat_id=None):
    chat_id = chat_id or str(conn.incr('ids:chat:'))      #A

    recipients.append(sender)                             #E
    recipientsd = dict((r, 0) for r in recipients)        #E

    pipeline = conn.pipeline(True)
    pipeline.zadd('chat:' + chat_id, **recipientsd)       #B
    for rec in recipients:                                #C
        pipeline.zadd('seen:' + rec, chat_id, 0)          #C
    pipeline.execute()

    return send_message(conn, chat_id, sender, message)   #D
# <end id="_1314_14473_9124"/>
#A Get a new chat id
#E Set up a dictionary of users to scores to add to the chat ZSET
#B Create the set with the list of people participating
#C Initialize the seen zsets
#D Send the message
#END

# <start id="_1314_14473_9127"/>
def send_message(conn, chat_id, sender, message):
    identifier = acquire_lock(conn, 'chat:' + chat_id)
    if not identifier:
        raise Exception("Couldn't get the lock")
    try:
        mid = conn.incr('ids:' + chat_id)                #A
        ts = time.time()                                 #A
        packed = json.dumps({                            #A
            'id': mid,                                   #A
            'ts': ts,                                    #A
            'sender': sender,                            #A
            'message': message,                          #A
        })                                               #A

        conn.zadd('msgs:' + chat_id, packed, mid)        #B
    finally:
        release_lock(conn, 'chat:' + chat_id, identifier)
    return chat_id
# <end id="_1314_14473_9127"/>
#A Prepare the message
#B Send the message to the chat
#END

# <start id="_1314_14473_9132"/>
def fetch_pending_messages(conn, recipient):
    seen = conn.zrange('seen:' + recipient, 0, -1, withscores=True) #A

    pipeline = conn.pipeline(True)

    for chat_id, seen_id in seen:                               #B
        pipeline.zrangebyscore(                                 #B
            'msgs:' + chat_id, seen_id+1, 'inf')                #B
    chat_info = zip(seen, pipeline.execute())                   #C

    for i, ((chat_id, seen_id), messages) in enumerate(chat_info):
        if not messages:
            continue
        messages[:] = map(json.loads, messages)
        seen_id = messages[-1]['id']                            #D
        conn.zadd('chat:' + chat_id, recipient, seen_id)        #D

        min_id = conn.zrange(                                   #E
            'chat:' + chat_id, 0, 0, withscores=True)           #E

        pipeline.zadd('seen:' + recipient, chat_id, seen_id)    #F
        if min_id:
            pipeline.zremrangebyscore(                          #G
                'msgs:' + chat_id, 0, min_id[0][1])             #G
        chat_info[i] = (chat_id, messages)
    pipeline.execute()

    return chat_info
# <end id="_1314_14473_9132"/>
#A Get the last message ids received
#B Fetch all new messages
#C Prepare information about the data to be returned
#D Update the 'chat' ZSET with the most recently received message
#E Discover messages that have been seen by all users
#F Update the 'seen' ZSET
#G Clean out messages that have been seen by all users
#END

# <start id="_1314_14473_9135"/>
def join_chat(conn, chat_id, user):
    message_id = int(conn.get('ids:' + chat_id))                #A

    pipeline = conn.pipeline(True)
    pipeline.zadd('chat:' + chat_id, user, message_id)          #B
    pipeline.zadd('seen:' + user, chat_id, message_id)          #C
    pipeline.execute()
# <end id="_1314_14473_9135"/>
#A Get the most recent message id for the chat
#B Add the user to the chat member list
#C Add the chat to the users's seen list
#END

# <start id="_1314_14473_9136"/>
def leave_chat(conn, chat_id, user):
    pipeline = conn.pipeline(True)
    pipeline.zrem('chat:' + chat_id, user)                      #A
    pipeline.zrem('seen:' + user, chat_id)                      #A
    pipeline.zcard('chat:' + chat_id)                           #B

    if not pipeline.execute()[-1]:
        pipeline.delete('msgs:' + chat_id)                      #C
        pipeline.delete('ids:' + chat_id)                       #C
        pipeline.execute()
    else:
        oldest = conn.zrange(                                   #D
            'chat:' + chat_id, 0, 0, withscores=True)           #D
        conn.zremrangebyscore('msgs:' + chat_id, 0, oldest[0][1])     #E
# <end id="_1314_14473_9136"/>
#A Remove the user from the chat
#B Find the number of remaining group members
#C Delete the chat
#D Find the oldest message seen by all users
#E Delete old messages from the chat
#END

# <start id="_1314_15044_3669"/>
aggregates = defaultdict(lambda: defaultdict(int))      #A

def daily_country_aggregate(conn, line):
    if line:
        line = line.split()
        ip = line[0]                                    #B
        day = line[1]                                   #B
        country = find_city_by_ip_local(ip)[2]          #C
        aggregates[day][country] += 1                   #D
        return

    for day, aggregate in aggregates.items():           #E
        conn.zadd('daily:country:' + day, **aggregate)  #E
        del aggregates[day]                             #E
# <end id="_1314_15044_3669"/>
#A Prepare the local aggregate dictionary
#B Extract the information from our log lines
#C Find the country from the IP address
#D Increment our local aggregate
#E The day file is done, write our aggregate to Redis
#END

# <start id="_1314_14473_9209"/>
def copy_logs_to_redis(conn, path, channel, count=10,
                       limit=2**30, quit_when_done=True):
    bytes_in_redis = 0
    waiting = deque()
    create_chat(conn, 'source', map(str, range(count)), '', channel) #I
    count = str(count)
    for logfile in sorted(os.listdir(path)):               #A
        full_path = os.path.join(path, logfile)

        fsize = os.stat(full_path).st_size
        while bytes_in_redis + fsize > limit:              #B
            cleaned = _clean(conn, channel, waiting, count)#B
            if cleaned:                                    #B
                bytes_in_redis -= cleaned                  #B
            else:                                          #B
                time.sleep(.25)                            #B

        with open(full_path, 'rb') as inp:                 #C
            block = ' '                                    #C
            while block:                                   #C
                block = inp.read(2**17)                    #C
                conn.append(channel+logfile, block)        #C

        send_message(conn, channel, 'source', logfile)     #D

        bytes_in_redis += fsize                            #E
        waiting.append((logfile, fsize))                   #E

    if quit_when_done:                                     #F
        send_message(conn, channel, 'source', ':done')     #F

    while waiting:                                         #G
        cleaned = _clean(conn, channel, waiting, count)    #G
        if cleaned:                                        #G
            bytes_in_redis -= cleaned                      #G
        else:                                              #G
            time.sleep(.25)                                #G

def _clean(conn, channel, waiting, count):                 #H
    if not waiting:                                        #H
        return 0                                           #H
    w0 = waiting[0][0]                                     #H
    if conn.get(channel + w0 + ':done') == count:          #H
        conn.delete(channel + w0, channel + w0 + ':done')  #H
        return waiting.popleft()[1]                        #H
    return 0                                               #H
# <end id="_1314_14473_9209"/>
#I Create the chat that will be used to send messages to clients
#A Iterate over all of the logfiles
#B Clean out finished files if we need more room
#C Upload the file to Redis
#D Notify the listeners that the file is ready
#E Update our local information about Redis' memory use
#F We are out of files, so signal that it is done
#G Clean up the files when we are done
#H How we actually perform the cleanup from Redis
#END

# <start id="_1314_14473_9213"/>
def process_logs_from_redis(conn, id, callback):
    while 1:
        fdata = fetch_pending_messages(conn, id)                    #A

        for ch, mdata in fdata:
            for message in mdata:
                logfile = message['message']

                if logfile == ':done':                                #B
                    return                                            #B
                elif not logfile:
                    continue

                block_reader = readblocks                             #C
                if logfile.endswith('.gz'):                           #C
                    block_reader = readblocks_gz                      #C

                for line in readlines(conn, ch+logfile, block_reader):#D
                    callback(conn, line)                              #E
                callback(conn, None)                                  #F

                conn.incr(ch + logfile + ':done')                     #G

        if not fdata:
            time.sleep(.1)
# <end id="_1314_14473_9213"/>
#A Fetch the list of files
#B No more logfiles
#C Choose a block reader
#D Iterate over the lines
#E Pass each line to the callback
#F Force a flush of our aggregate caches
#G Report that we are finished with the log
#END

# <start id="_1314_14473_9221"/>
def readlines(conn, key, rblocks):
    out = ''
    for block in rblocks(conn, key):
        out += block
        posn = out.rfind('\n')                      #A
        if posn >= 0:                               #B
            for line in out[:posn].split('\n'):     #C
                yield line + '\n'                   #D
            out = out[posn+1:]                      #E
        if not block:                               #F
            yield out
            break
# <end id="_1314_14473_9221"/>
#A Find the rightmost linebreak if any - rfind() returns -1 on failure
#B We found a line break
#C Split on all of the line breaks
#D Yield each line
#E Keep track of the trailing data
#F We are out of data
#END

# <start id="_1314_14473_9225"/>
def readblocks(conn, key, blocksize=2**17):
    lb = blocksize
    pos = 0
    while lb == blocksize:                                  #A
        block = conn.substr(key, pos, pos + blocksize - 1)  #B
        yield block                                         #C
        lb = len(block)                                     #C
        pos += lb                                           #C
    yield ''
# <end id="_1314_14473_9225"/>
#A Keep going while we got as much as we expected
#B Fetch the block
#C Prepare for the next pass
#END

# <start id="_1314_14473_9229"/>
def readblocks_gz(conn, key):
    inp = ''
    decoder = None
    for block in readblocks(conn, key, 2**17):                  #A
        if not decoder:
            inp += block
            try:
                if inp[:3] != "\x1f\x8b\x08":                #B
                    raise IOError("invalid gzip data")          #B
                i = 10                                          #B
                flag = ord(inp[3])                              #B
                if flag & 4:                                    #B
                    i += 2 + ord(inp[i]) + 256*ord(inp[i+1])    #B
                if flag & 8:                                    #B
                    i = inp.index('\0', i) + 1                  #B
                if flag & 16:                                   #B
                    i = inp.index('\0', i) + 1                  #B
                if flag & 2:                                    #B
                    i += 2                                      #B

                if i > len(inp):                                #C
                    raise IndexError("not enough data")         #C
            except (IndexError, ValueError):                    #C
                continue                                        #C

            else:
                block = inp[i:]                                 #D
                inp = None                                      #D
                decoder = zlib.decompressobj(-zlib.MAX_WBITS)   #D
                if not block:
                    continue

        if not block:                                           #E
            yield decoder.flush()                               #E
            break

        yield decoder.decompress(block)                         #F
# <end id="_1314_14473_9229"/>
#A Read the raw data from Redis
#B Parse the header information so that we can get the compressed data
#C We haven't read the full header yet
#D We found the header, prepare the decompressor
#E We are out of data, yield the last chunk
#F Yield a decompressed block
#END

class TestCh06(unittest.TestCase):
    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)

    def tearDown(self):
        self.conn.flushdb()
        del self.conn
        print
        print

    def test_add_update_contact(self):
        import pprint
        conn = self.conn
        conn.delete('recent:user')

        print "Let's add a few contacts..."
        for i in xrange(10):
            add_update_contact(conn, 'user', 'contact-%i-%i'%(i//3, i))
        print "Current recently contacted contacts"
        contacts = conn.lrange('recent:user', 0, -1)
        pprint.pprint(contacts)
        self.assertTrue(len(contacts) >= 10)
        print

        print "Let's pull one of the older ones up to the front"
        add_update_contact(conn, 'user', 'contact-1-4')
        contacts = conn.lrange('recent:user', 0, 2)
        print "New top-3 contacts:"
        pprint.pprint(contacts)
        self.assertEquals(contacts[0], 'contact-1-4')
        print

        print "Let's remove a contact..."
        print remove_contact(conn, 'user', 'contact-2-6')
        contacts = conn.lrange('recent:user', 0, -1)
        print "New contacts:"
        pprint.pprint(contacts)
        self.assertTrue(len(contacts) >= 9)
        print

        print "And let's finally autocomplete on "
        all = conn.lrange('recent:user', 0, -1)
        contacts = fetch_autocomplete_list(conn, 'user', 'c')
        self.assertTrue(all == contacts)
        equiv = [c for c in all if c.startswith('contact-2-')]
        contacts = fetch_autocomplete_list(conn, 'user', 'contact-2-')
        equiv.sort()
        contacts.sort()
        self.assertEquals(equiv, contacts)
        conn.delete('recent:user')

    def test_address_book_autocomplete(self):
        self.conn.delete('members:test')
        print "the start/end range of 'abc' is:", find_prefix_range('abc')
        print

        print "Let's add a few people to the guild"
        for name in ['jeff', 'jenny', 'jack', 'jennifer']:
            join_guild(self.conn, 'test', name)
        print
        print "now let's try to find users with names starting with 'je':"
        r = autocomplete_on_prefix(self.conn, 'test', 'je')
        print r
        self.assertTrue(len(r) == 3)
        print "jeff just left to join a different guild..."
        leave_guild(self.conn, 'test', 'jeff')
        r = autocomplete_on_prefix(self.conn, 'test', 'je')
        print r
        self.assertTrue(len(r) == 2)
        self.conn.delete('members:test')

    def test_distributed_locking(self):
        self.conn.delete('lock:testlock')
        print "Getting an initial lock..."
        self.assertTrue(acquire_lock_with_timeout(self.conn, 'testlock', 1, 1))
        print "Got it!"
        print "Trying to get it again without releasing the first one..."
        self.assertFalse(acquire_lock_with_timeout(self.conn, 'testlock', .01, 1))
        print "Failed to get it!"
        print
        print "Waiting for the lock to timeout..."
        time.sleep(2)
        print "Getting the lock again..."
        r = acquire_lock_with_timeout(self.conn, 'testlock', 1, 1)
        self.assertTrue(r)
        print "Got it!"
        print "Releasing the lock..."
        self.assertTrue(release_lock(self.conn, 'testlock', r))
        print "Released it..."
        print
        print "Acquiring it again..."
        self.assertTrue(acquire_lock_with_timeout(self.conn, 'testlock', 1, 1))
        print "Got it!"
        self.conn.delete('lock:testlock')

    def test_counting_semaphore(self):
        self.conn.delete('testsem', 'testsem:owner', 'testsem:counter')
        print "Getting 3 initial semaphores with a limit of 3..."
        for i in xrange(3):
            self.assertTrue(acquire_fair_semaphore(self.conn, 'testsem', 3, 1))
        print "Done!"
        print "Getting one more that should fail..."
        self.assertFalse(acquire_fair_semaphore(self.conn, 'testsem', 3, 1))
        print "Couldn't get it!"
        print
        print "Lets's wait for some of them to time out"
        time.sleep(2)
        print "Can we get one?"
        r = acquire_fair_semaphore(self.conn, 'testsem', 3, 1)
        self.assertTrue(r)
        print "Got one!"
        print "Let's release it..."
        self.assertTrue(release_fair_semaphore(self.conn, 'testsem', r))
        print "Released!"
        print
        print "And let's make sure we can get 3 more!"
        for i in xrange(3):
            self.assertTrue(acquire_fair_semaphore(self.conn, 'testsem', 3, 1))
        print "We got them!"
        self.conn.delete('testsem', 'testsem:owner', 'testsem:counter')

    def test_delayed_tasks(self):
        import threading
        self.conn.delete('queue:tqueue', 'delayed:')
        print "Let's start some regular and delayed tasks..."
        for delay in [0, .5, 0, 1.5]:
            self.assertTrue(execute_later(self.conn, 'tqueue', 'testfn', [], delay))
        r = self.conn.llen('queue:tqueue')
        print "How many non-delayed tasks are there (should be 2)?", r
        self.assertEquals(r, 2)
        print
        print "Let's start up a thread to bring those delayed tasks back..."
        t = threading.Thread(target=poll_queue, args=(self.conn,))
        t.setDaemon(1)
        t.start()
        print "Started."
        print "Let's wait for those tasks to be prepared..."
        time.sleep(2)
        global QUIT
        QUIT = True
        t.join()
        r = self.conn.llen('queue:tqueue')
        print "Waiting is over, how many tasks do we have (should be 4)?", r
        self.assertEquals(r, 4)
        self.conn.delete('queue:tqueue', 'delayed:')

    def test_multi_recipient_messaging(self):
        self.conn.delete('ids:chat:', 'msgs:1', 'ids:1', 'seen:joe', 'seen:jeff', 'seen:jenny')

        print "Let's create a new chat session with some recipients..."
        chat_id = create_chat(self.conn, 'joe', ['jeff', 'jenny'], 'message 1')
        print "Now let's send a few messages..."
        for i in xrange(2, 5):
            send_message(self.conn, chat_id, 'joe', 'message %s'%i)
        print
        print "And let's get the messages that are waiting for jeff and jenny..."
        r1 = fetch_pending_messages(self.conn, 'jeff')
        r2 = fetch_pending_messages(self.conn, 'jenny')
        print "They are the same?", r1==r2
        self.assertEquals(r1, r2)
        print "Those messages are:"
        import pprint
        pprint.pprint(r1)
        self.conn.delete('ids:chat:', 'msgs:1', 'ids:1', 'seen:joe', 'seen:jeff', 'seen:jenny')

    def test_file_distribution(self):
        import gzip, shutil, tempfile, threading
        self.conn.delete('test:temp-1.txt', 'test:temp-2.txt', 'test:temp-3.txt', 'msgs:test:', 'seen:0', 'seen:source', 'ids:test:', 'chat:test:')

        dire = tempfile.mkdtemp()
        try:
            print "Creating some temporary 'log' files..."
            with open(dire + '/temp-1.txt', 'wb') as f:
                f.write('one line\n')
            with open(dire + '/temp-2.txt', 'wb') as f:
                f.write(10000 * 'many lines\n')
            out = gzip.GzipFile(dire + '/temp-3.txt.gz', mode='wb')
            for i in xrange(100000):
                out.write('random line %s\n'%(os.urandom(16).encode('hex'),))
            out.close()
            size = os.stat(dire + '/temp-3.txt.gz').st_size
            print "Done."
            print
            print "Starting up a thread to copy logs to redis..."
            t = threading.Thread(target=copy_logs_to_redis, args=(self.conn, dire, 'test:', 1, size))
            t.setDaemon(1)
            t.start()

            print "Let's pause to let some logs get copied to Redis..."
            time.sleep(.25)
            print
            print "Okay, the logs should be ready. Let's process them!"

            index = [0]
            counts = [0, 0, 0]
            def callback(conn, line):
                if line is None:
                    print "Finished with a file %s, linecount: %s"%(index[0], counts[index[0]])
                    index[0] += 1
                elif line or line.endswith('\n'):
                    counts[index[0]] += 1

            print "Files should have 1, 10000, and 100000 lines"
            process_logs_from_redis(self.conn, '0', callback)
            self.assertEquals(counts, [1, 10000, 100000])

            print
            print "Let's wait for the copy thread to finish cleaning up..."
            t.join()
            print "Done cleaning out Redis!"

        finally:
            print "Time to clean up files..."
            shutil.rmtree(dire)
            print "Cleaned out files!"
        self.conn.delete('test:temp-1.txt', 'test:temp-2.txt', 'test:temp-3.txt', 'msgs:test:', 'seen:0', 'seen:source', 'ids:test:', 'chat:test:')

if __name__ == '__main__':
    unittest.main()
