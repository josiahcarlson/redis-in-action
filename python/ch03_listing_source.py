
import threading
import time
import unittest

import redis

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432
ARTICLES_PER_PAGE = 25

'''
# <start id="string-calls-1"/>
>>> conn = redis.Redis()
>>> conn.get('key')             #A
>>> conn.incr('key')            #B
1                               #B
>>> conn.incr('key', 15)        #B
16                              #B
>>> conn.decr('key', 5)         #C
11                              #C
>>> conn.get('key')             #D
'11'                            #D
>>> conn.set('key', '13')       #E
True                            #E
>>> conn.incr('key')            #E
14                              #E
# <end id="string-calls-1"/>
#A When we fetch a key that does not exist, we get the None value, which is not displayed in the interactive console
#B We can increment keys that don't exist, and we can pass an optional value to increment by more than 1
#C Like incrementing, decrementing takes an optional argument for the amount to decrement by
#D When we fetch the key it acts like a string
#E And when we set the key, we can set it as a string, but still manipulate it like an integer
#END
'''


'''
# <start id="string-calls-2"/>
>>> conn.append('new-string-key', 'hello ')     #A
6L                                              #B
>>> conn.append('new-string-key', 'world!')
12L                                             #B
>>> conn.substr('new-string-key', 3, 7)         #C
'lo wo'                                         #D
>>> conn.setrange('new-string-key', 0, 'H')     #E
12                                              #F
>>> conn.setrange('new-string-key', 6, 'W')
12
>>> conn.get('new-string-key')                  #G
'Hello World!'                                  #H
>>> conn.setrange('new-string-key', 11, ', how are you?')   #I
25
>>> conn.get('new-string-key')
'Hello World, how are you?'                     #J
>>> conn.setbit('another-key', 2, 1)            #K
0                                               #L
>>> conn.setbit('another-key', 7, 1)            #M
0                                               #M
>>> conn.get('another-key')                     #M
'!'                                             #N
# <end id="string-calls-2"/>
#A Let's append the string 'hello ' to the previously non-existent key 'new-string-key'
#B When appending a value, Redis returns the length of the string so far
#C Redis uses 0-indexing, and when accessing ranges, is inclusive of the endpoints by default
#D The string 'lo wo' is from the middle of 'hello world!'
#E Let's set a couple string ranges
#F When setting a range inside a string, Redis also returns the total length of the string
#G Let's see what we have now!
#H Yep, we capitalized our 'H' and 'W'
#I With setrange we can replace anywhere inside the string, and we can make the string longer
#J We replaced the exclamation point and added more to the end of the string
#K If you write to a bit beyond the size of the string, it is filled with nulls
#L Setting bits also returns the value of the bit before it was set
#M If you are going to try to interpret the bits stored in Redis, remember that offsets into bits are from the highest-order to the lowest-order
#N We set bits 2 and 7 to 1, which gave us '!', or character 33
#END
'''

'''
# <start id="list-calls-1"/>
>>> conn.rpush('list-key', 'last')          #A
1L                                          #A
>>> conn.lpush('list-key', 'first')         #B
2L
>>> conn.rpush('list-key', 'new last')
3L
>>> conn.lrange('list-key', 0, -1)          #C
['first', 'last', 'new last']               #C
>>> conn.lpop('list-key')                   #D
'first'                                     #D
>>> conn.lpop('list-key')                   #D
'last'                                      #D
>>> conn.lrange('list-key', 0, -1)
['new last']
>>> conn.rpush('list-key', 'a', 'b', 'c')   #E
4L
>>> conn.lrange('list-key', 0, -1)
['new last', 'a', 'b', 'c']
>>> conn.ltrim('list-key', 2, -1)           #F
True                                        #F
>>> conn.lrange('list-key', 0, -1)          #F
['b', 'c']                                  #F
# <end id="list-calls-1"/>
#A When we push items onto the list, it returns the length of the list after the push has completed
#B We can easily push on both ends of the list
#C Semantically, the left end of the list is the beginning, and the right end of the list is the end
#D Popping off the left items repeatedly will return items from left to right
#E We can push multiple items at the same time
#F We can trim any number of items from the start, end, or both
#END
'''

'''
# <start id="list-calls-2"/>
>>> conn.rpush('list', 'item1')             #A
1                                           #A
>>> conn.rpush('list', 'item2')             #A
2                                           #A
>>> conn.rpush('list2', 'item3')            #A
1                                           #A
>>> conn.brpoplpush('list2', 'list', 1)     #B
'item3'                                     #B
>>> conn.brpoplpush('list2', 'list', 1)     #C
>>> conn.lrange('list', 0, -1)              #D
['item3', 'item1', 'item2']                 #D
>>> conn.brpoplpush('list', 'list2', 1)
'item2'
>>> conn.blpop(['list', 'list2'], 1)        #E
('list', 'item3')                           #E
>>> conn.blpop(['list', 'list2'], 1)        #E
('list', 'item1')                           #E
>>> conn.blpop(['list', 'list2'], 1)        #E
('list2', 'item2')                          #E
>>> conn.blpop(['list', 'list2'], 1)        #E
>>>
# <end id="list-calls-2"/>
#A Let's add some items to a couple lists to start
#B Let's move an item from one list to the other, leaving it
#C When a list is empty, the blocking pop will stall for the timeout, and return None (which is not displayed in the interactive console)
#D We popped the rightmost item from 'list2' and pushed it to the left of 'list'
#E Blocking left-popping items from these will check lists for items in the order that they are passed, until they are empty
#END
'''

# <start id="exercise-update-token"/>
def update_token(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        key = 'viewed:' + token
        conn.lrem(key, item)                    #A
        conn.rpush(key, item)                   #B
        conn.ltrim(key, -25, -1)                #C
        conn.zincrby('viewed:', item, -1)
# <end id="exercise-update-token"/>
#A Remove the item from the list if it was there
#B Push the item to the right side of the LIST so that ZRANGE and LRANGE have the same result
#C Trim the LIST to only include the most recent 25 items
#END


'''
# <start id="set-calls-1"/>
>>> conn.sadd('set-key', 'a', 'b', 'c')         #A
3                                               #A
>>> conn.srem('set-key', 'c', 'd')              #B
True                                            #B
>>> conn.srem('set-key', 'c', 'd')              #B
False                                           #B
>>> conn.scard('set-key')                       #C
2                                               #C
>>> conn.smembers('set-key')                    #D
set(['a', 'b'])                                 #D
>>> conn.smove('set-key', 'set-key2', 'a')      #E
True                                            #E
>>> conn.smove('set-key', 'set-key2', 'c')      #F
False                                           #F
>>> conn.smembers('set-key2')                   #F
set(['a'])                                      #F
# <end id="set-calls-1"/>
#A Adding items to the SET returns the number of items that weren't already in the SET
#B Removing items from the SET returns whether an item was removed - note that the client is buggy in that respect, as Redis itself returns the total number of items removed
#C We can get the number of items in the SET
#D We can also fetch the whole SET
#E We can easily move items from one SET to another SET
#F When an item doesn't exist in the first set during a SMOVE, it isn't added to the destination SET
#END
'''


'''
# <start id="set-calls-2"/>
>>> conn.sadd('skey1', 'a', 'b', 'c', 'd')  #A
4                                           #A
>>> conn.sadd('skey2', 'c', 'd', 'e', 'f')  #A
4                                           #A
>>> conn.sdiff('skey1', 'skey2')            #B
set(['a', 'b'])                             #B
>>> conn.sinter('skey1', 'skey2')           #C
set(['c', 'd'])                             #C
>>> conn.sunion('skey1', 'skey2')           #D
set(['a', 'c', 'b', 'e', 'd', 'f'])         #D
# <end id="set-calls-2"/>
#A First we'll add a few items to a couple SETs
#B We can calculate the result of removing all of the items in the second set from the first SET
#C We can also find out which items exist in both SETs
#D And we can find out all of the items that are in either of the SETs
#END
'''

'''
# <start id="hash-calls-1"/>
>>> conn.hmset('hash-key', {'k1':'v1', 'k2':'v2', 'k3':'v3'})   #A
True                                                            #A
>>> conn.hmget('hash-key', ['k2', 'k3'])                        #B
['v2', 'v3']                                                    #B
>>> conn.hlen('hash-key')                                       #C
3                                                               #C
>>> conn.hdel('hash-key', 'k1', 'k3')                           #D
True                                                            #D
# <end id="hash-calls-1"/>
#A We can add multiple items to the hash in one call
#B We can fetch a subset of the values in a single call
#C The HLEN command is typically used for debugging very large HASHes
#D The HDEL command handles multiple arguments without needing an HMDEL counterpart and returns True if any fields were removed
#END
'''

'''
# <start id="hash-calls-2"/>
>>> conn.hmset('hash-key2', {'short':'hello', 'long':1000*'1'}) #A
True                                                            #A
>>> conn.hkeys('hash-key2')                                     #A
['long', 'short']                                               #A
>>> conn.hexists('hash-key2', 'num')                            #B
False                                                           #B
>>> conn.hincrby('hash-key2', 'num')                            #C
1L                                                              #C
>>> conn.hexists('hash-key2', 'num')                            #C
True                                                            #C
# <end id="hash-calls-2"/>
#A Fetching keys can be useful to keep from needing to transfer large values when you are looking into HASHes
#B We can also check the existence of specific keys
#C Incrementing a previously non-existent key in a hash behaves just like on strings, Redis operates as though the value had been 0
#END
'''

'''
# <start id="zset-calls-1"/>
>>> conn.zadd('zset-key', 'a', 3, 'b', 2, 'c', 1)   #A
3                                                   #A
>>> conn.zcard('zset-key')                          #B
3                                                   #B
>>> conn.zincrby('zset-key', 'c', 3)                #C
4.0                                                 #C
>>> conn.zscore('zset-key', 'b')                    #D
2.0                                                 #D
>>> conn.zrank('zset-key', 'c')                     #E
2                                                   #E
>>> conn.zcount('zset-key', 0, 3)                   #F
2L                                                  #F
>>> conn.zrem('zset-key', 'b')                      #G
True                                                #G
>>> conn.zrange('zset-key', 0, -1, withscores=True) #H
[('a', 3.0), ('c', 4.0)]                            #H
# <end id="zset-calls-1"/>
#A Adding members to ZSETs in Python has the arguments reversed compared to standard Redis, so as to not confuse users compared to HASHes
#B Knowing how large a ZSET is can tell you in some cases if it is necessary to trim your ZSET
#C We can also increment members like we can with STRING and HASH values
#D Fetching scores of individual members can be useful if you have been keeping counters or toplists
#E By fetching the 0-indexed position of a member, we can then later use ZRANGE to fetch a range of the values easily
#F Counting the number of items with a given range of scores can be quite useful for some tasks
#G Removing members is as easy as adding them
#H For debugging, we usually fetch the entire ZSET with this ZRANGE call, but real use-cases will usually fetch items a relatively small group at a time
#END
'''

'''
# <start id="zset-calls-2"/>
>>> conn.zadd('zset-1', 'a', 1, 'b', 2, 'c', 3)                         #A
3                                                                       #A
>>> conn.zadd('zset-2', 'b', 4, 'c', 1, 'd', 0)                         #A
3                                                                       #A
>>> conn.zinterstore('zset-i', ['zset-1', 'zset-2'])                    #B
2L                                                                      #B
>>> conn.zrange('zset-i', 0, -1, withscores=True)                       #B
[('c', 4.0), ('b', 6.0)]                                                #B
>>> conn.zunionstore('zset-u', ['zset-1', 'zset-2'], aggregate='min')   #C
4L                                                                      #C
>>> conn.zrange('zset-u', 0, -1, withscores=True)                       #C
[('d', 0.0), ('a', 1.0), ('c', 1.0), ('b', 2.0)]                        #C
>>> conn.sadd('set-1', 'a', 'd')                                        #D
2                                                                       #D
>>> conn.zunionstore('zset-u2', ['zset-1', 'zset-2', 'set-1'])          #D
4L                                                                      #D
>>> conn.zrange('zset-u2', 0, -1, withscores=True)                      #D
[('d', 1.0), ('a', 2.0), ('c', 4.0), ('b', 6.0)]                        #D
# <end id="zset-calls-2"/>
#A We'll start out by creating a couple ZSETs
#B When performing ZINTERSTORE or ZUNIONSTORE, our default aggregate is sum, so scores of items that are in multiple ZSETs are added
#C It is easy to provide different aggregates, though we are limited to sum, min, and max
#D You can also pass SETs as inputs to ZINTERSTORE and ZUNIONSTORE, they behave as though they were ZSETs with all scores equal to 1
#END
'''

def publisher(n):
    time.sleep(1)
    for i in xrange(n):
        conn.publish('channel', i)
        time.sleep(1)

def run_pubsub():
    threading.Thread(target=publisher, args=(3,)).start()
    pubsub = conn.pubsub()
    pubsub.subscribe(['channel'])
    count = 0
    for item in pubsub.listen():
        print item
        count += 1
        if count == 4:
            pubsub.unsubscribe()
        if count == 5:
            break

'''
# <start id="pubsub-calls-1"/>
>>> def publisher(n):
...     time.sleep(1)                                                   #A
...     for i in xrange(n):
...         conn.publish('channel', i)                                  #B
...         time.sleep(1)                                               #B
...
>>> def run_pubsub():
...     threading.Thread(target=publisher, args=(3,)).start()
...     pubsub = conn.pubsub()
...     pubsub.subscribe(['channel'])
...     count = 0
...     for item in pubsub.listen():
...         print item
...         count += 1
...         if count == 4:
...             pubsub.unsubscribe()
...         if count == 5:
...             break
... 

>>> def run_pubsub():
...     threading.Thread(target=publisher, args=(3,)).start()           #D
...     pubsub = conn.pubsub()                                          #E
...     pubsub.subscribe(['channel'])                                   #E
...     count = 0
...     for item in pubsub.listen():                                    #F
...         print item                                                  #G
...         count += 1                                                  #H
...         if count == 4:                                              #H
...             pubsub.unsubscribe()                                    #H
...         if count == 5:                                              #L
...             break                                                   #L
...
>>> run_pubsub()                                                        #C
{'pattern': None, 'type': 'subscribe', 'channel': 'channel', 'data': 1L}#I
{'pattern': None, 'type': 'message', 'channel': 'channel', 'data': '0'} #J
{'pattern': None, 'type': 'message', 'channel': 'channel', 'data': '1'} #J
{'pattern': None, 'type': 'message', 'channel': 'channel', 'data': '2'} #J
{'pattern': None, 'type': 'unsubscribe', 'channel': 'channel', 'data':  #K
0L}                                                                     #K
# <end id="pubsub-calls-1"/>
#A We sleep initially in the function to let the SUBSCRIBEr connect and start listening for messages
#B After publishing, we will pause for a moment so that we can see this happen over time
#D Let's start the publisher thread to send 3 messages
#E We'll set up the pubsub object and subscribe to a channel
#F We can listen to subscription messages by iterating over the result of pubsub.listen()
#G We'll print every message that we receive
#H We will stop listening for new messages after the subscribe message and 3 real messages by unsubscribing
#L When we receive the unsubscribe message, we need to stop receiving messages
#C Actually run the functions to see them work
#I When subscribing, we receive a message on the listen channel
#J These are the structures that are produced as items when we iterate over pubsub.listen()
#K When we unsubscribe, we receive a message telling us which channels we have unsubscribed from and the number of channels we are still subscribed to
#END
'''


'''
# <start id="sort-calls"/>
>>> conn.rpush('sort-input', 23, 15, 110, 7)                    #A
4                                                               #A
>>> conn.sort('sort-input')                                     #B
['7', '15', '23', '110']                                        #B
>>> conn.sort('sort-input', alpha=True)                         #C
['110', '15', '23', '7']                                        #C
>>> conn.hset('d-7', 'field', 5)                                #D
1L                                                              #D
>>> conn.hset('d-15', 'field', 1)                               #D
1L                                                              #D
>>> conn.hset('d-23', 'field', 9)                               #D
1L                                                              #D
>>> conn.hset('d-110', 'field', 3)                              #D
1L                                                              #D
>>> conn.sort('sort-input', by='d-*->field')                    #E
['15', '110', '7', '23']                                        #E
>>> conn.sort('sort-input', by='d-*->field', get='d-*->field')  #F
['1', '3', '5', '9']                                            #F
# <end id="sort-calls"/>
#A Start by adding some items to a LIST
#B We can sort the items numerically
#C And we can sort the items alphabetically
#D We are just adding some additional data for SORTing and fetching
#E We can sort our data by fields of HASHes
#F And we can even fetch that data and return it instead of or in addition to our input data
#END
'''

'''
# <start id="simple-pipeline-notrans"/>
>>> def notrans():
...     print conn.incr('notrans:')                     #A
...     time.sleep(.1)                                  #B
...     conn.incr('notrans:', -1)                       #C
...
>>> if 1:
...     for i in xrange(3):                             #D
...         threading.Thread(target=notrans).start()    #D
...     time.sleep(.5)                                  #E
...
1                                                       #F
2                                                       #F
3                                                       #F
# <end id="simple-pipeline-notrans"/>
#A Increment the 'notrans:' counter and print the result
#B Wait for 100 milliseconds
#C Decrement the 'notrans:' counter
#D Start three threads to execute the non-transactional increment/sleep/decrement
#E Wait half a second for everything to be done
#F Because there is no transaction, each of the threaded commands can interleave freely, causing the counter to steadily grow in this case
#END
'''

'''
# <start id="simple-pipeline-trans"/>
>>> def trans():
...     pipeline = conn.pipeline()                      #A
...     pipeline.incr('trans:')                         #B
...     time.sleep(.1)                                  #C
...     pipeline.incr('trans:', -1)                     #D
...     print pipeline.execute()[0]                     #E
...
>>> if 1:
...     for i in xrange(3):                             #F
...         threading.Thread(target=trans).start()      #F
...     time.sleep(.5)                                  #G
...
1                                                       #H
1                                                       #H
1                                                       #H
# <end id="simple-pipeline-trans"/>
#A Create a transactional pipeline
#B Queue up the 'trans:' counter increment
#C Wait for 100 milliseconds
#D Queue up the 'trans:' counter decrement
#E Execute both commands and print the result of the increment operation
#F Start three of the transactional increment/sleep/decrement calls
#G Wait half a second for everything to be done
#H Because each increment/sleep/decrement pair is executed inside a transaction, no other commands can be interleaved, which gets us a result of 1 for all of our results
#END
'''

# <start id="exercise-fix-article-vote"/>
def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    posted = conn.zscore('time:', article)                      #A
    if posted < cutoff:
        return

    article_id = article.partition(':')[-1]
    pipeline = conn.pipeline()
    pipeline.sadd('voted:' + article_id, user)
    pipeline.expire('voted:' + article_id, int(posted-cutoff))  #B
    if pipeline.execute()[0]:
        pipeline.zincrby('score:', article, VOTE_SCORE)         #C
        pipeline.hincrby(article, 'votes', 1)                   #C
        pipeline.execute()                                      #C
# <end id="exercise-fix-article-vote"/>
#A If the article should expire bewteen our ZSCORE and our SADD, we need to use the posted time to properly expire it
#B Set the expiration time if we shouldn't have actually added the vote to the SET
#C We could lose our connection between the SADD/EXPIRE and ZINCRBY/HINCRBY, so the vote may not count, but that is better than it partially counting by failing between the ZINCRBY/HINCRBY calls
#END

# Technically, the above article_vote() version still has some issues, which
# are addressed in the following, which uses features/functionality not
# introduced until chapter 4.

def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    posted = conn.zscore('time:', article)
    article_id = article.partition(':')[-1]
    voted = 'voted:' + article_id

    pipeline = conn.pipeline()
    while posted > cutoff:
        try:
            pipeline.watch(voted)
            if not pipeline.sismember(voted, user):
                pipeline.multi()
                pipeline.sadd(voted, user)
                pipeline.expire(voted, int(posted-cutoff))
                pipeline.zincrby('score:', article, VOTE_SCORE)
                pipeline.hincrby(article, 'votes', 1)
                pipeline.execute()
            else:
                pipeline.unwatch()
            return
        except redis.exceptions.WatchError:
            cutoff = time.time() - ONE_WEEK_IN_SECONDS

# <start id="exercise-fix-get_articles"/>
def get_articles(conn, page, order='score:'):
    start = max(page-1, 0) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    ids = conn.zrevrangebyscore(order, start, end)

    pipeline = conn.pipeline()
    map(pipeline.hgetall, ids)                              #A

    articles = []
    for id, article_data in zip(ids, pipeline.execute()):   #B
        article_data['id'] = id
        articles.append(article_data)

    return articles
# <end id="exercise-fix-get_articles"/>
#A Prepare the HGETALL calls on the pipeline
#B Execute the pipeline and add ids to the article
#END

'''
# <start id="other-calls-1"/>
>>> conn.set('key', 'value')                    #A
True                                            #A
>>> conn.get('key')                             #A
'value'                                         #A
>>> conn.expire('key', 2)                       #B
True                                            #B
>>> time.sleep(2)                               #B
>>> conn.get('key')                             #B
>>> conn.set('key', 'value2')
True
>>> conn.expire('key', 100); conn.ttl('key')    #C
True                                            #C
100                                             #C
# <end id="other-calls-1"/>
#A We are starting with a very simple STRING value
#B If we set a key to expire in the future, and we wait long enough for the key to expire, when we try to fetch the key, it has already been deleted
#C We can also easily find out how long it will be before a key will expire
#END
'''

# <start id="exercise-no-recent-zset"/>
THIRTY_DAYS = 30*86400
def check_token(conn, token):
    return conn.get('login:' + token)       #A

def update_token(conn, token, user, item=None):
    conn.setex('login:' + token, user, THIRTY_DAYS) #B
    key = 'viewed:' + token
    if item:
        conn.lrem(key, item)
        conn.rpush(key, item)
        conn.ltrim(key, -25, -1)
        conn.zincrby('viewed:', item, -1)
    conn.expire(key, THIRTY_DAYS)                   #C

def add_to_cart(conn, session, item, count):
    key = 'cart:' + session
    if count <= 0:
        conn.hrem(key, item)
    else:
        conn.hset(key, item, count)
    conn.expire(key, THIRTY_DAYS)               #D
# <end id="exercise-no-recent-zset"/>
#A We are going to store the login token as a string value so we can EXPIRE it
#B Set the value of the the login token and the token's expiration time with one call
#C We can't manipulate LISTs and set their expiration at the same time, so we must do it later
#D We also can't manipulate HASHes and set their expiration times, so we again do it later
#END
