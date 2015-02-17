
import time
import unittest

'''
# <start id="simple-string-calls"/>
$ redis-cli                                 #A
redis 127.0.0.1:6379> set hello world       #D
OK                                          #E
redis 127.0.0.1:6379> get hello             #F
"world"                                     #G
redis 127.0.0.1:6379> del hello             #H
(integer) 1                                 #I
redis 127.0.0.1:6379> get hello             #J
(nil)
redis 127.0.0.1:6379> 
# <end id="simple-string-calls"/>
#A Start the redis-cli client up
#D Set the key 'hello' to the value 'world'
#E If a SET command succeeds, it returns 'OK', which turns into True on the Python side
#F Now get the value stored at the key 'hello'
#G It is still 'world', like we just set it
#H Let's delete the key/value pair
#I If there was a value to delete, DEL returns the number of items that were deleted
#J There is no more value, so trying to fetch the value returns nil, which turns into None on the Python side
#END
'''


'''
# <start id="simple-list-calls"/>
redis 127.0.0.1:6379> rpush list-key item   #A
(integer) 1                                 #A
redis 127.0.0.1:6379> rpush list-key item2  #A
(integer) 2                                 #A
redis 127.0.0.1:6379> rpush list-key item   #A
(integer) 3                                 #A
redis 127.0.0.1:6379> lrange list-key 0 -1  #B
1) "item"                                   #B
2) "item2"                                  #B
3) "item"                                   #B
redis 127.0.0.1:6379> lindex list-key 1     #C
"item2"                                     #C
redis 127.0.0.1:6379> lpop list-key         #D
"item"                                      #D
redis 127.0.0.1:6379> lrange list-key 0 -1  #D
1) "item2"                                  #D
2) "item"                                   #D
redis 127.0.0.1:6379> 
# <end id="simple-list-calls"/>
#A When we push items onto a LIST, the command returns the current length of the list
#B We can fetch the entire list by passing a range of 0 for the start index, and -1 for the last index
#C We can fetch individual items from the list with LINDEX
#D By popping an item from the list, it is no longer available
#END
'''


'''
# <start id="simple-set-calls"/>
redis 127.0.0.1:6379> sadd set-key item     #A
(integer) 1                                 #A
redis 127.0.0.1:6379> sadd set-key item2    #A
(integer) 1                                 #A
redis 127.0.0.1:6379> sadd set-key item3    #A
(integer) 1                                 #A
redis 127.0.0.1:6379> sadd set-key item     #A
(integer) 0                                 #A
redis 127.0.0.1:6379> smembers set-key      #B
1) "item"                                   #B
2) "item2"                                  #B
3) "item3"                                  #B
redis 127.0.0.1:6379> sismember set-key item4   #C
(integer) 0                                     #C
redis 127.0.0.1:6379> sismember set-key item    #C
(integer) 1                                     #C
redis 127.0.0.1:6379> srem set-key item2    #D
(integer) 1                                 #D
redis 127.0.0.1:6379> srem set-key item2    #D
(integer) 0                                 #D
redis 127.0.0.1:6379>  smembers set-key
1) "item"
2) "item3"
redis 127.0.0.1:6379> 
# <end id="simple-set-calls"/>
#A When adding an item to a SET, Redis will return a 1 if the item is new to the set and 0 if it was already in the SET
#B We can fetch all of the items in the SET, which returns them as a sequence of items, which is turned into a Python set from Python
#C We can also ask Redis whether an item is in the SET, which turns into a boolean in Python
#D When we attempt to remove items, our commands return the number of items that were removed
#END
'''


'''
# <start id="simple-hash-calls"/>
redis 127.0.0.1:6379> hset hash-key sub-key1 value1 #A
(integer) 1                                         #A
redis 127.0.0.1:6379> hset hash-key sub-key2 value2 #A
(integer) 1                                         #A
redis 127.0.0.1:6379> hset hash-key sub-key1 value1 #A
(integer) 0                                         #A
redis 127.0.0.1:6379> hgetall hash-key              #B
1) "sub-key1"                                       #B
2) "value1"                                         #B
3) "sub-key2"                                       #B
4) "value2"                                         #B
redis 127.0.0.1:6379> hdel hash-key sub-key2        #C
(integer) 1                                         #C
redis 127.0.0.1:6379> hdel hash-key sub-key2        #C
(integer) 0                                         #C
redis 127.0.0.1:6379> hget hash-key sub-key1        #D
"value1"                                            #D
redis 127.0.0.1:6379> hgetall hash-key
1) "sub-key1"
2) "value1"
# <end id="simple-hash-calls"/>
#A When we add items to a hash, again we get a return value that tells us whether the item is new in the hash
#B We can fetch all of the items in the HASH, which gets translated into a dictionary on the Python side of things
#C When we delete items from the hash, the command returns whether the item was there before we tried to remove it
#D We can also fetch individual fields from hashes
#END
'''


'''
# <start id="simple-zset-calls"/>
redis 127.0.0.1:6379> zadd zset-key 728 member1     #A
(integer) 1                                         #A
redis 127.0.0.1:6379> zadd zset-key 982 member0     #A
(integer) 1                                         #A
redis 127.0.0.1:6379> zadd zset-key 982 member0     #A
(integer) 0                                         #A
redis 127.0.0.1:6379> zrange zset-key 0 -1 withscores   #B
1) "member1"                                            #B
2) "728"                                                #B
3) "member0"                                            #B
4) "982"                                                #B
redis 127.0.0.1:6379> zrangebyscore zset-key 0 800 withscores   #C
1) "member1"                                                    #C
2) "728"                                                        #C
redis 127.0.0.1:6379> zrem zset-key member1     #D
(integer) 1                                     #D
redis 127.0.0.1:6379> zrem zset-key member1     #D
(integer) 0                                     #D
redis 127.0.0.1:6379> zrange zset-key 0 -1 withscores
1) "member0"
2) "982"
# <end id="simple-zset-calls"/>
#A When we add items to a ZSET, the the command returns the number of new items
#B We can fetch all of the items in the ZSET, which are ordered by the scores, and scores are turned into floats in Python
#C We can also fetch a subsequence of items based on their scores
#D When we remove items, we again find the number of items that were removed
#END
'''

# <start id="upvote-code"/>
ONE_WEEK_IN_SECONDS = 7 * 86400                     #A
VOTE_SCORE = 432                                    #A

def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS      #B
    if conn.zscore('time:', article) < cutoff:      #C
        return

    article_id = article.partition(':')[-1]         #D
    if conn.sadd('voted:' + article_id, user):      #E
        conn.zincrby('score:', article, VOTE_SCORE) #E
        conn.hincrby(article, 'votes', 1)           #E
# <end id="upvote-code"/>
#A Prepare our constants
#B Calculate the cutoff time for voting
#C Check to see if the article can still be voted on (we could use the article HASH here, but scores are returned as floats so we don't have to cast it)
#D Get the id portion from the article:id identifier
#E If the user hasn't voted for this article before, increment the article score and vote count (note that our HINCRBY and ZINCRBY calls should be in a Redis transaction, but we don't introduce them until chapter 3 and 4, so ignore that for now)
#END

# <start id="post-article-code"/>
def post_article(conn, user, title, link):
    article_id = str(conn.incr('article:'))     #A

    voted = 'voted:' + article_id
    conn.sadd(voted, user)                      #B
    conn.expire(voted, ONE_WEEK_IN_SECONDS)     #B

    now = time.time()
    article = 'article:' + article_id
    conn.hmset(article, {                       #C
        'title': title,                         #C
        'link': link,                           #C
        'poster': user,                         #C
        'time': now,                            #C
        'votes': 1,                             #C
    })                                          #C

    conn.zadd('score:', article, now + VOTE_SCORE)  #D
    conn.zadd('time:', article, now)                #D

    return article_id
# <end id="post-article-code"/>
#A Generate a new article id
#B Start with the posting user having voted for the article, and set the article voting information to automatically expire in a week (we discuss expiration in chapter 3)
#C Create the article hash
#D Add the article to the time and score ordered zsets
#END

# <start id="fetch-articles-code"/>
ARTICLES_PER_PAGE = 25

def get_articles(conn, page, order='score:'):
    start = (page-1) * ARTICLES_PER_PAGE            #A
    end = start + ARTICLES_PER_PAGE - 1             #A

    ids = conn.zrevrange(order, start, end)         #B
    articles = []
    for id in ids:                                  #C
        article_data = conn.hgetall(id)             #C
        article_data['id'] = id                     #C
        articles.append(article_data)               #C

    return articles
# <end id="fetch-articles-code"/>
#A Set up the start and end indexes for fetching the articles
#B Fetch the article ids
#C Get the article information from the list of article ids
#END

# <start id="add-remove-groups"/>
def add_remove_groups(conn, article_id, to_add=[], to_remove=[]):
    article = 'article:' + article_id           #A
    for group in to_add:
        conn.sadd('group:' + group, article)    #B
    for group in to_remove:
        conn.srem('group:' + group, article)    #C
# <end id="add-remove-groups"/>
#A Construct the article information like we did in post_article
#B Add the article to groups that it should be a part of
#C Remove the article from groups that it should be removed from
#END

# <start id="fetch-articles-group"/>
def get_group_articles(conn, group, page, order='score:'):
    key = order + group                                     #A
    if not conn.exists(key):                                #B
        conn.zinterstore(key,                               #C
            ['group:' + group, order],                      #C
            aggregate='max',                                #C
        )
        conn.expire(key, 60)                                #D
    return get_articles(conn, page, key)                    #E
# <end id="fetch-articles-group"/>
#A Create a key for each group and each sort order
#B If we haven't sorted these articles recently, we should sort them
#C Actually sort the articles in the group based on score or recency
#D Tell Redis to automatically expire the ZSET in 60 seconds
#E Call our earlier get_articles() function to handle pagination and article data fetching
#END

#--------------- Below this line are helpers to test the code ----------------

class TestCh01(unittest.TestCase):
    def setUp(self):
        import redis
        self.conn = redis.Redis(db=15)

    def tearDown(self):
        del self.conn
        print
        print

    def test_article_functionality(self):
        conn = self.conn
        import pprint

        article_id = str(post_article(conn, 'username', 'A title', 'http://www.google.com'))
        print "We posted a new article with id:", article_id
        print
        self.assertTrue(article_id)

        print "Its HASH looks like:"
        r = conn.hgetall('article:' + article_id)
        print r
        print
        self.assertTrue(r)

        article_vote(conn, 'other_user', 'article:' + article_id)
        print "We voted for the article, it now has votes:",
        v = int(conn.hget('article:' + article_id, 'votes'))
        print v
        print
        self.assertTrue(v > 1)

        print "The currently highest-scoring articles are:"
        articles = get_articles(conn, 1)
        pprint.pprint(articles)
        print

        self.assertTrue(len(articles) >= 1)

        add_remove_groups(conn, article_id, ['new-group'])
        print "We added the article to a new group, other articles include:"
        articles = get_group_articles(conn, 'new-group', 1)
        pprint.pprint(articles)
        print
        self.assertTrue(len(articles) >= 1)

        to_del = (
            conn.keys('time:*') + conn.keys('voted:*') + conn.keys('score:*') + 
            conn.keys('article:*') + conn.keys('group:*')
        )
        if to_del:
            conn.delete(*to_del)

if __name__ == '__main__':
    unittest.main()
