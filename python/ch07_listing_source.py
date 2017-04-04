
import math
import re
import unittest
import uuid

import redis

AVERAGE_PER_1K = {}

# <start id="tokenize-and-index"/>
STOP_WORDS = set('''able about across after all almost also am among
an and any are as at be because been but by can cannot could dear did
do does either else ever every for from get got had has have he her
hers him his how however if in into is it its just least let like
likely may me might most must my neither no nor not of off often on
only or other our own rather said say says she should since so some
than that the their them then there these they this tis to too twas us
wants was we were what when where which while who whom why will with
would yet you your'''.split())                                          #A

WORDS_RE = re.compile("[a-z']{2,}")                                     #B

def tokenize(content):
    words = set()                                                       #C
    for match in WORDS_RE.finditer(content.lower()):                    #D
        word = match.group().strip("'")                                 #E
        if len(word) >= 2:                                              #F
            words.add(word)                                             #F
    return words - STOP_WORDS                                           #G

def index_document(conn, docid, content):
    words = tokenize(content)                                           #H

    pipeline = conn.pipeline(True)
    for word in words:                                                  #I
        pipeline.sadd('idx:' + word, docid)                             #I
    return len(pipeline.execute())                                      #J
# <end id="tokenize-and-index"/>
#A We pre-declare our known stop words, these were fetched from http://www.textfixer.com/resources/
#B A regular expression that extracts words as we defined them
#C Our Python set of words that we have found in the document content
#D Iterate over all of the words in the content
#E Strip any leading or trailing single-quote characters
#F Keep any words that are still at least 2 characters long
#G Return the set of words that remain that are also not stop words
#H Get the tokenized words for the content
#I Add the documents to the appropriate inverted index entries
#J Return the number of unique non-stop words that were added for the document
#END

# <start id="_1314_14473_9158"/>
def _set_common(conn, method, names, ttl=30, execute=True):
    id = str(uuid.uuid4())                                  #A
    pipeline = conn.pipeline(True) if execute else conn     #B
    names = ['idx:' + name for name in names]               #C
    getattr(pipeline, method)('idx:' + id, *names)          #D
    pipeline.expire('idx:' + id, ttl)                       #E
    if execute:
        pipeline.execute()                                  #F
    return id                                               #G

def intersect(conn, items, ttl=30, _execute=True):          #H
    return _set_common(conn, 'sinterstore', items, ttl, _execute) #H

def union(conn, items, ttl=30, _execute=True):                    #I
    return _set_common(conn, 'sunionstore', items, ttl, _execute) #I

def difference(conn, items, ttl=30, _execute=True):               #J
    return _set_common(conn, 'sdiffstore', items, ttl, _execute)  #J
# <end id="_1314_14473_9158"/>
#A Create a new temporary identifier
#B Set up a transactional pipeline so that we have consistent results for each individual call
#C Add the 'idx:' prefix to our terms
#D Set up the call for one of the operations
#E Instruct Redis to expire the SET in the future
#F Actually execute the operation
#G Return the id for the caller to process the results
#H Helper function to perform SET intersections
#I Helper function to perform SET unions
#J Helper function to perform SET differences
#END

# <start id="parse-query"/>
QUERY_RE = re.compile("[+-]?[a-z']{2,}")                #A

def parse(query):
    unwanted = set()                                    #B
    all = []                                            #C
    current = set()                                     #D
    for match in QUERY_RE.finditer(query.lower()):      #E
        word = match.group()                            #F
        prefix = word[:1]                               #F
        if prefix in '+-':                              #F
            word = word[1:]                             #F
        else:                                           #F
            prefix = None                               #F

        word = word.strip("'")                          #G
        if len(word) < 2 or word in STOP_WORDS:         #G
            continue                                    #G

        if prefix == '-':                               #H
            unwanted.add(word)                          #H
            continue                                    #H

        if current and not prefix:                      #I
            all.append(list(current))                   #I
            current = set()                             #I
        current.add(word)                               #J

    if current:                                         #K
        all.append(list(current))                       #K

    return all, list(unwanted)                          #L
# <end id="parse-query"/>
#A Our regular expression for finding wanted, unwanted, and synonym words
#B A unique set of unwanted words
#C Our final result of words that we are looking to intersect
#D The current unique set of words to consider as synonyms
#E Iterate over all words in the search query
#F Discover +/- prefixes, if any
#G Strip any leading or trailing single quotes, and skip anything that is a stop word
#H If the word is unwanted, add it to the unwanted set
#I Set up a new synonym set if we have no synonym prefix and we already have words
#J Add the current word to the current set
#K Add any remaining words to the final intersection
#END

# <start id="search-query"/>
def parse_and_search(conn, query, ttl=30):
    all, unwanted = parse(query)                                    #A
    if not all:                                                     #B
        return None                                                 #B

    to_intersect = []
    for syn in all:                                                 #D
        if len(syn) > 1:                                            #E
            to_intersect.append(union(conn, syn, ttl=ttl))          #E
        else:                                                       #F
            to_intersect.append(syn[0])                             #F

    if len(to_intersect) > 1:                                       #G
        intersect_result = intersect(conn, to_intersect, ttl=ttl)   #G
    else:                                                           #H
        intersect_result = to_intersect[0]                          #H

    if unwanted:                                                    #I
        unwanted.insert(0, intersect_result)                        #I
        return difference(conn, unwanted, ttl=ttl)                  #I

    return intersect_result                                         #J
# <end id="search-query"/>
#A Parse the query
#B If there are no words in the query that are not stop words, we don't have a result
#D Iterate over each list of synonyms
#E If the synonym list is more than one word long, then perform the union operation
#F Otherwise use the individual word directly
#G If we have more than one word/result to intersect, intersect them
#H Otherwise use the individual word/result directly
#I If we have any unwanted words, remove them from our earlier result and return it
#J Otherwise return the intersection result
#END


# <start id="sorted-searches"/>
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
# <end id="sorted-searches"/>
#A We will optionally take an previous result id, a way to sort the results, and options for paginating over the results
#B Determine which attribute to sort by, and whether to sort ascending or descending
#I We need to tell Redis whether we are sorting by a number or alphabetically
#C If there was a previous result, try to update its expiration time if it still exists
#D Perform the search if we didn't have a past search id, or if our results expired
#E Fetch the total number of results
#F Sort the result list by the proper column and fetch only those results we want
#G Return the number of items in the results, the results we wanted, and the id of the results so that we can fetch them again later
#END

# <start id="zset_scored_composite"/>
def search_and_zsort(conn, query, id=None, ttl=300, update=1, vote=0,   #A
                    start=0, num=20, desc=True):                        #A

    if id and not conn.expire(id, ttl):     #B
        id = None                           #B

    if not id:                                      #C
        id = parse_and_search(conn, query, ttl=ttl) #C

        scored_search = {
            id: 0,                                  #I
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
# <end id="zset_scored_composite"/>
#A Like before, we'll optionally take a previous result id for pagination if the result is still available
#B We will refresh the search result's TTL if possible
#C If our search result expired, or if this is the first time we've searched, perform the standard SET search
#I We use the 'id' key for the intersection, but we don't want it to count towards weights
#D Set up the scoring adjustments for balancing update time and votes. Remember: votes can be adjusted to 1, 10, 100, or higher depending on the sorting result desired.
#E Intersect using our helper function that we define in listing 7.7
#F Fetch the size of the result ZSET
#G Handle fetching a "page" of results
#H Return the results and the id for pagination
#END


# <start id="zset_helpers"/>
def _zset_common(conn, method, scores, ttl=30, **kw):
    id = str(uuid.uuid4())                                  #A
    execute = kw.pop('_execute', True)                      #J
    pipeline = conn.pipeline(True) if execute else conn     #B
    for key in scores.keys():                               #C
        scores['idx:' + key] = scores.pop(key)              #C
    getattr(pipeline, method)('idx:' + id, scores, **kw)    #D
    pipeline.expire('idx:' + id, ttl)                       #E
    if execute:                                             #F
        pipeline.execute()                                  #F
    return id                                               #G

def zintersect(conn, items, ttl=30, **kw):                              #H
    return _zset_common(conn, 'zinterstore', dict(items), ttl, **kw)    #H

def zunion(conn, items, ttl=30, **kw):                                  #I
    return _zset_common(conn, 'zunionstore', dict(items), ttl, **kw)    #I
# <end id="zset_helpers"/>
#A Create a new temporary identifier
#B Set up a transactional pipeline so that we have consistent results for each individual call
#C Add the 'idx:' prefix to our inputs
#D Set up the call for one of the operations
#E Instruct Redis to expire the ZSET in the future
#F Actually execute the operation, unless explicitly instructed not to by the caller
#G Return the id for the caller to process the results
#H Helper function to perform ZSET intersections
#I Helper function to perform ZSET unions
#J Allow the passing of an argument to determine whether we should defer pipeline execution
#END


# <start id="string-to-score"/>
def string_to_score(string, ignore_case=False):
    if ignore_case:                         #A
        string = string.lower()             #A

    pieces = map(ord, string[:6])           #B
    while len(pieces) < 6:                  #C
        pieces.append(-1)                   #C

    score = 0
    for piece in pieces:                    #D
        score = score * 257 + piece + 1     #D

    return score * 2 + (len(string) > 6)    #E
# <end id="string-to-score"/>
#A We can handle optional case-insensitive indexes easily, so we will
#B Convert the first 6 characters of the string into their numeric values, null being 0, tab being 9, capital A being 65, etc.
#C For strings that aren't at least 6 characters long, we will add place-holder values to represent that the string was short
#D For each value in the converted string values, we add it to the score, taking into consideration that a null is different from a place holder
#E Because we have an extra bit, we can also signify whether the string is exactly 6 characters or more, allowing us to differentiate 'robber' and 'robbers', though not 'robbers' and 'robbery'
#END

def to_char_map(set):
    out = {}
    for pos, val in enumerate(sorted(set)):
        out[val] = pos-1
    return out

LOWER = to_char_map(set([-1]) | set(xrange(ord('a'), ord('z')+1)))
ALPHA = to_char_map(set(LOWER) | set(xrange(ord('A'), ord('Z')+1)))
LOWER_NUMERIC = to_char_map(set(LOWER) | set(xrange(ord('0'), ord('9')+1)))
ALPHA_NUMERIC = to_char_map(set(LOWER_NUMERIC) | set(ALPHA))

def string_to_score_generic(string, mapping):
    length = int(52 / math.log(len(mapping), 2))    #A

    pieces = map(ord, string[:length])              #B
    while len(pieces) < length:                     #C
        pieces.append(-1)                           #C

    score = 0
    for piece in pieces:                            #D
        value = mapping[piece]                      #D
        score = score * len(mapping) + value + 1    #D

    return score * 2 + (len(string) > length)       #E



# <start id="zadd-string"/>
def zadd_string(conn, name, *args, **kwargs):
    pieces = list(args)                         #A
    for piece in kwargs.iteritems():            #A
        pieces.extend(piece)                    #A

    for i, v in enumerate(pieces):
        if i & 1:                               #B
            pieces[i] = string_to_score(v)      #B

    return conn.zadd(name, *pieces)             #C
# <end id="zadd-string"/>
#A Combine both types of arguments passed for later modification
#B Convert string scores to integer scores
#C Call the existing ZADD method
#END

# <start id="ecpm_helpers"/>
def cpc_to_ecpm(views, clicks, cpc):
    return 1000. * cpc * clicks / views

def cpa_to_ecpm(views, actions, cpa):
    return 1000. * cpa * actions / views #A
# <end id="ecpm_helpers"/>
#A Because click through rate is (clicks/views), and action rate is (actions/clicks), when we multiply them together we get (actions/views)
#END

# <start id="index_ad"/>
TO_ECPM = {
    'cpc': cpc_to_ecpm,
    'cpa': cpa_to_ecpm,
    'cpm': lambda *args:args[-1],
}

def index_ad(conn, id, locations, content, type, value):
    pipeline = conn.pipeline(True)                          #A

    for location in locations:
        pipeline.sadd('idx:req:'+location, id)              #B

    words = tokenize(content)
    for word in words:                                      #H
        pipeline.zadd('idx:' + word, id, 0)                 #H

    rvalue = TO_ECPM[type](                                 #C
        1000, AVERAGE_PER_1K.get(type, 1), value)           #C
    pipeline.hset('type:', id, type)                        #D
    pipeline.zadd('idx:ad:value:', id, rvalue)              #E
    pipeline.zadd('ad:base_value:', id, value)              #F
    pipeline.sadd('terms:' + id, *list(words))              #G
    pipeline.execute()
# <end id="index_ad"/>
#A Set up the pipeline so that we only need a single round-trip to perform the full index operation
#B Add the ad id to all of the relevant location SETs for targeting
#H Index the words for the ad
#C We will keep a dictionary that stores the average number of clicks or actions per 1000 views on our network, for estimating the performance of new ads
#D Record what type of ad this is
#E Add the ad's eCPM to a ZSET of all ads
#F Add the ad's base value to a ZST of all ads
#G Keep a record of the words that could be targeted for the ad
#END

# <start id="target_ad"/>
def target_ads(conn, locations, content):
    pipeline = conn.pipeline(True)
    matched_ads, base_ecpm = match_location(pipeline, locations)    #A
    words, targeted_ads = finish_scoring(                           #B
        pipeline, matched_ads, base_ecpm, content)                  #B

    pipeline.incr('ads:served:')                                    #C
    pipeline.zrevrange('idx:' + targeted_ads, 0, 0)                 #D
    target_id, targeted_ad = pipeline.execute()[-2:]

    if not targeted_ad:                                             #E
        return None, None                                           #E

    ad_id = targeted_ad[0]
    record_targeting_result(conn, target_id, ad_id, words)          #F

    return target_id, ad_id                                         #G
# <end id="target_ad"/>
#A Find all ads that fit the location targeting parameter, and their eCPMs
#B Finish any bonus scoring based on matching the content
#C Get an id that can be used for reporting and recording of this particular ad target
#D Fetch the top-eCPM ad id
#E If there were no ads that matched the location targeting, return nothing
#F Record the results of our targeting efforts as part of our learning process
#G Return the target id and the ad id to the caller
#END

# <start id="location_target"/>
def match_location(pipe, locations):
    required = ['req:' + loc for loc in locations]                  #A
    matched_ads = union(pipe, required, ttl=300, _execute=False)    #B
    return matched_ads, zintersect(pipe,                            #C
        {matched_ads: 0, 'ad:value:': 1}, _execute=False)  #C
# <end id="location_target"/>
#A Calculate the SET key names for all of the provided locations
#B Calculate the SET of matched ads that are valid for this location
#C Return the matched ads SET id, as well as the id of the ZSET that includes the base eCPM of all of the matched ads
#END

# <start id="finish_scoring"/>
def finish_scoring(pipe, matched, base, content):
    bonus_ecpm = {}
    words = tokenize(content)                                   #A
    for word in words:
        word_bonus = zintersect(                                #B
            pipe, {matched: 0, word: 1}, _execute=False)        #B
        bonus_ecpm[word_bonus] = 1                              #B

    if bonus_ecpm:
        minimum = zunion(                                       #C
            pipe, bonus_ecpm, aggregate='MIN', _execute=False)  #C
        maximum = zunion(                                       #C
            pipe, bonus_ecpm, aggregate='MAX', _execute=False)  #C

        return words, zunion(                                       #D
            pipe, {base:1, minimum:.5, maximum:.5}, _execute=False) #D
    return words, base                                          #E
# <end id="finish_scoring"/>
#A Tokenize the content for matching against ads
#B Find the ads that are location-targeted, which also have one of the words in the content
#C Find the minimum and maximum eCPM bonuses for each ad
#D Compute the total of the base + half of the minimum eCPM bonus + half of the maximum eCPM bonus
#E If there were no words in the content to match against, return just the known eCPM
#END

# <start id="record_targeting"/>
def record_targeting_result(conn, target_id, ad_id, words):
    pipeline = conn.pipeline(True)

    terms = conn.smembers('terms:' + ad_id)                 #A
    matched = list(words & terms)                           #A
    if matched:
        matched_key = 'terms:matched:%s' % target_id
        pipeline.sadd(matched_key, *matched)                #B
        pipeline.expire(matched_key, 900)                   #B

    type = conn.hget('type:', ad_id)                        #C
    pipeline.incr('type:%s:views:' % type)                  #C
    for word in matched:                                    #D
        pipeline.zincrby('views:%s' % ad_id, word)          #D
    pipeline.zincrby('views:%s' % ad_id, '')                #D

    if not pipeline.execute()[-1] % 100:                    #E
        update_cpms(conn, ad_id)                            #E

# <end id="record_targeting"/>
#A Find the words in the content that matched with the words in the ad
#B If any words in the ad matched the content, record that information and keep it for 15 minutes
#C Keep a per-type count of the number of views that each ad received
#D Record view information for each word in the ad, as well as the ad itself
#E Every 100th time that the ad was shown, update the ad's eCPM
#END

# <start id="record_click"/>
def record_click(conn, target_id, ad_id, action=False):
    pipeline = conn.pipeline(True)
    click_key = 'clicks:%s'%ad_id

    match_key = 'terms:matched:%s'%target_id

    type = conn.hget('type:', ad_id)
    if type == 'cpa':                       #A
        pipeline.expire(match_key, 900)     #A
        if action:
            click_key = 'actions:%s' % ad_id  #B

    if action and type == 'cpa':
        pipeline.incr('type:%s:actions:' % type) #C
    else:
        pipeline.incr('type:%s:clicks:' % type)   #C

    matched = list(conn.smembers(match_key))#D
    matched.append('')                      #D
    for word in matched:                    #D
        pipeline.zincrby(click_key, word)   #D
    pipeline.execute()

    update_cpms(conn, ad_id)                #E
# <end id="record_click"/>
#A If the ad was a CPA ad, refresh the expiration time of the matched terms if it is still available
#B Record actions instead of clicks
#C Keep a global count of clicks/actions for ads based on the ad type
#D Record clicks (or actions) for the ad and for all words that had been targeted in the ad
#E Update the eCPM for all words that were seen in the ad
#END

# <start id="update_cpms"/>
def update_cpms(conn, ad_id):
    pipeline = conn.pipeline(True)
    pipeline.hget('type:', ad_id)               #A
    pipeline.zscore('ad:base_value:', ad_id)    #A
    pipeline.smembers('terms:' + ad_id)         #A
    type, base_value, words = pipeline.execute()#A

    which = 'clicks'                                        #B
    if type == 'cpa':                                       #B
        which = 'actions'                                   #B

    pipeline.get('type:%s:views:' % type)                   #C
    pipeline.get('type:%s:%s' % (type, which))              #C
    type_views, type_clicks = pipeline.execute()            #C
    AVERAGE_PER_1K[type] = (                                        #D
        1000. * int(type_clicks or '1') / int(type_views or '1'))   #D

    if type == 'cpm':   #E
        return          #E

    view_key = 'views:%s' % ad_id
    click_key = '%s:%s' % (which, ad_id)

    to_ecpm = TO_ECPM[type]

    pipeline.zscore(view_key, '')                                   #G
    pipeline.zscore(click_key, '')                                  #G
    ad_views, ad_clicks = pipeline.execute()                        #G
    if (ad_clicks or 0) < 1:                                        #N
        ad_ecpm = conn.zscore('idx:ad:value:', ad_id)               #N
    else:
        ad_ecpm = to_ecpm(ad_views or 1, ad_clicks or 0, base_value)#H
        pipeline.zadd('idx:ad:value:', ad_id, ad_ecpm)              #H

    for word in words:
        pipeline.zscore(view_key, word)                             #I
        pipeline.zscore(click_key, word)                            #I
        views, clicks = pipeline.execute()[-2:]                     #I

        if (clicks or 0) < 1:                                       #J
            continue                                                #J

        word_ecpm = to_ecpm(views or 1, clicks or 0, base_value)    #K
        bonus = word_ecpm - ad_ecpm                                 #L
        pipeline.zadd('idx:' + word, ad_id, bonus)                  #M
    pipeline.execute()
# <end id="update_cpms"/>
#A Fetch the type and value of the ad, as well as all of the words in the ad
#B Determine whether the eCPM of the ad should be based on clicks or actions
#C Fetch the current number of views and clicks/actions for the given ad type
#D Write back to our global dictionary the click-through rate or action rate for the ad
#E If we are processing a CPM ad, then we don't update any of the eCPMs, as they are already updated
#N Use the existing eCPM if the ad hasn't received any clicks yet
#G Fetch the per-ad view and click/action scores and
#H Calculate the ad's eCPM and update the ad's value
#I Fetch the view and click/action scores for the word
#J Don't update eCPMs when the ad has not received any clicks
#K Calculate the word's eCPM
#L Calculate the word's bonus
#M Write the word's bonus back to the per-word per-ad ZSET
#END


# <start id="slow_job_search"/>
def add_job(conn, job_id, required_skills):
    conn.sadd('job:' + job_id, *required_skills)        #A

def is_qualified(conn, job_id, candidate_skills):
    temp = str(uuid.uuid4())
    pipeline = conn.pipeline(True)
    pipeline.sadd(temp, *candidate_skills)              #B
    pipeline.expire(temp, 5)                            #B
    pipeline.sdiff('job:' + job_id, temp)               #C
    return not pipeline.execute()[-1]                   #D
# <end id="slow_job_search"/>
#A Add all required job skills to the job's SET
#B Add the candidate's skills to a temporary SET with an expiration time
#C Calculate the SET of skills that the job requires that the user doesn't have
#D Return True if there are no skills that the candidate does not have
#END

# <start id="job_search_index"/>
def index_job(conn, job_id, skills):
    pipeline = conn.pipeline(True)
    for skill in skills:
        pipeline.sadd('idx:skill:' + skill, job_id)             #A
    pipeline.zadd('idx:jobs:req', job_id, len(set(skills)))     #B
    pipeline.execute()
# <end id="job_search_index"/>
#A Add the job id to all appropriate skill SETs
#B Add the total required skill count to the required skills ZSET
#END

# <start id="job_search_results"/>
def find_jobs(conn, candidate_skills):
    skills = {}                                                 #A
    for skill in set(candidate_skills):                         #A
        skills['skill:' + skill] = 1                            #A

    job_scores = zunion(conn, skills)                           #B
    final_result = zintersect(                                  #C
        conn, {job_scores:-1, 'jobs:req':1})                    #C

    return conn.zrangebyscore('idx:' + final_result, 0, 0)      #D
# <end id="job_search_results"/>
#A Set up the dictionary for scoring the jobs
#B Calculate the scores for each of the jobs
#C Calculate how many more skills the job requires than the candidate has
#D Return the jobs that the candidate has the skills for
#END

# 0 is beginner, 1 is intermediate, 2 is expert
SKILL_LEVEL_LIMIT = 2

def index_job_levels(conn, job_id, skill_levels):
    total_skills = len(set(skill for skill, level in skill_levels))
    pipeline = conn.pipeline(True)
    for skill, level in skill_levels:
        level = min(level, SKILL_LEVEL_LIMIT)
        for wlevel in xrange(level, SKILL_LEVEL_LIMIT+1):
            pipeline.sadd('idx:skill:%s:%s'%(skill,wlevel), job_id)
    pipeline.zadd('idx:jobs:req', job_id, total_skills)
    pipeline.execute()

def search_job_levels(conn, skill_levels):
    skills = {}
    for skill, level in skill_levels:
        level = min(level, SKILL_LEVEL_LIMIT)
        skills['skill:%s:%s'%(skill,level)] = 1

    job_scores = zunion(conn, skills)
    final_result = zintersect(conn, {job_scores:-1, 'jobs:req':1})

    return conn.zrangebyscore('idx:' + final_result, '-inf', 0)


def index_job_years(conn, job_id, skill_years):
    total_skills = len(set(skill for skill, years in skill_years))
    pipeline = conn.pipeline(True)
    for skill, years in skill_years:
        pipeline.zadd(
            'idx:skill:%s:years'%skill, job_id, max(years, 0))
    pipeline.sadd('idx:jobs:all', job_id)
    pipeline.zadd('idx:jobs:req', job_id, total_skills)
    pipeline.execute()

def search_job_years(conn, skill_years):
    skill_years = dict(skill_years)
    pipeline = conn.pipeline(True)

    union = []
    for skill, years in skill_years.iteritems():
        sub_result = zintersect(pipeline,
            {'jobs:all':-years, 'skill:%s:years'%skill:1}, _execute=False)
        pipeline.zremrangebyscore('idx:' + sub_result, '(0', 'inf')
        union.append(
            zintersect(pipeline, {'jobs:all':1, sub_result:0}, _execute=False))

    job_scores = zunion(pipeline, dict((key, 1) for key in union), _execute=False)
    final_result = zintersect(pipeline, {job_scores:-1, 'jobs:req':1}, _execute=False)

    pipeline.zrangebyscore('idx:' + final_result, '-inf', 0)
    return pipeline.execute()[-1]


class TestCh07(unittest.TestCase):
    content = 'this is some random content, look at how it is indexed.'
    def setUp(self):
        self.conn = redis.Redis(db=15)
        self.conn.flushdb()
    def tearDown(self):
        self.conn.flushdb()

    def test_index_document(self):
        print "We're tokenizing some content..."
        tokens = tokenize(self.content)
        print "Those tokens are:", tokens
        self.assertTrue(tokens)

        print "And now we are indexing that content..."
        r = index_document(self.conn, 'test', self.content)
        self.assertEquals(r, len(tokens))
        for t in tokens:
            self.assertEquals(self.conn.smembers('idx:' + t), set(['test']))

    def test_set_operations(self):
        index_document(self.conn, 'test', self.content)

        r = intersect(self.conn, ['content', 'indexed'])
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = intersect(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set())

        r = union(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = difference(self.conn, ['content', 'ignored'])
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = difference(self.conn, ['content', 'indexed'])
        self.assertEquals(self.conn.smembers('idx:' + r), set())

    def test_parse_query(self):
        query = 'test query without stopwords'
        self.assertEquals(parse(query), ([[x] for x in query.split()], []))

        query = 'test +query without -stopwords'
        self.assertEquals(parse(query), ([['test', 'query'], ['without']], ['stopwords']))

    def test_parse_and_search(self):
        print "And now we are testing search..."
        index_document(self.conn, 'test', self.content)

        r = parse_and_search(self.conn, 'content')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content indexed random')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content +indexed random')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content indexed +random')
        self.assertEquals(self.conn.smembers('idx:' + r), set(['test']))

        r = parse_and_search(self.conn, 'content indexed -random')
        self.assertEquals(self.conn.smembers('idx:' + r), set())

        print "Which passed!"

    def test_search_with_sort(self):
        print "And now let's test searching with sorting..."

        index_document(self.conn, 'test', self.content)
        index_document(self.conn, 'test2', self.content)
        self.conn.hmset('kb:doc:test', {'updated': 12345, 'id': 10})
        self.conn.hmset('kb:doc:test2', {'updated': 54321, 'id': 1})

        r = search_and_sort(self.conn, "content")
        self.assertEquals(r[1], ['test2', 'test'])

        r = search_and_sort(self.conn, "content", sort='-id')
        self.assertEquals(r[1], ['test', 'test2'])
        print "Which passed!"

    def test_search_with_zsort(self):
        print "And now let's test searching with sorting via zset..."

        index_document(self.conn, 'test', self.content)
        index_document(self.conn, 'test2', self.content)
        self.conn.zadd('idx:sort:update', 'test', 12345, 'test2', 54321)
        self.conn.zadd('idx:sort:votes', 'test', 10, 'test2', 1)

        r = search_and_zsort(self.conn, "content", desc=False)
        self.assertEquals(r[1], ['test', 'test2'])

        r = search_and_zsort(self.conn, "content", update=0, vote=1, desc=False)
        self.assertEquals(r[1], ['test2', 'test'])
        print "Which passed!"

    def test_string_to_score(self):
        words = 'these are some words that will be sorted'.split()
        pairs = [(word, string_to_score(word)) for word in words]
        pairs2 = list(pairs)
        pairs.sort()
        pairs2.sort(key=lambda x:x[1])
        self.assertEquals(pairs, pairs2)

        words = 'these are some words that will be sorted'.split()
        pairs = [(word, string_to_score_generic(word, LOWER)) for word in words]
        pairs2 = list(pairs)
        pairs.sort()
        pairs2.sort(key=lambda x:x[1])
        self.assertEquals(pairs, pairs2)

        zadd_string(self.conn, 'key', 'test', 'value', test2='other')
        self.assertEquals(self.conn.zscore('key', 'test'), string_to_score('value'))
        self.assertEquals(self.conn.zscore('key', 'test2'), string_to_score('other'))

    def test_index_and_target_ads(self):
        index_ad(self.conn, '1', ['USA', 'CA'], self.content, 'cpc', .25)
        index_ad(self.conn, '2', ['USA', 'VA'], self.content + ' wooooo', 'cpc', .125)

        for i in xrange(100):
            ro = target_ads(self.conn, ['USA'], self.content)
        self.assertEquals(ro[1], '1')

        r = target_ads(self.conn, ['VA'], 'wooooo')
        self.assertEquals(r[1], '2')

        self.assertEquals(self.conn.zrange('idx:ad:value:', 0, -1, withscores=True), [('2', 0.125), ('1', 0.25)])
        self.assertEquals(self.conn.zrange('ad:base_value:', 0, -1, withscores=True), [('2', 0.125), ('1', 0.25)])

        record_click(self.conn, ro[0], ro[1])

        self.assertEquals(self.conn.zrange('idx:ad:value:', 0, -1, withscores=True), [('2', 0.125), ('1', 2.5)])
        self.assertEquals(self.conn.zrange('ad:base_value:', 0, -1, withscores=True), [('2', 0.125), ('1', 0.25)])

    def test_is_qualified_for_job(self):
        add_job(self.conn, 'test', ['q1', 'q2', 'q3'])
        self.assertTrue(is_qualified(self.conn, 'test', ['q1', 'q3', 'q2']))
        self.assertFalse(is_qualified(self.conn, 'test', ['q1', 'q2']))

    def test_index_and_find_jobs(self):
        index_job(self.conn, 'test1', ['q1', 'q2', 'q3'])
        index_job(self.conn, 'test2', ['q1', 'q3', 'q4'])
        index_job(self.conn, 'test3', ['q1', 'q3', 'q5'])

        self.assertEquals(find_jobs(self.conn, ['q1']), [])
        self.assertEquals(find_jobs(self.conn, ['q1', 'q3', 'q4']), ['test2'])
        self.assertEquals(find_jobs(self.conn, ['q1', 'q3', 'q5']), ['test3'])
        self.assertEquals(find_jobs(self.conn, ['q1', 'q2', 'q3', 'q4', 'q5']), ['test1', 'test2', 'test3'])

    def test_index_and_find_jobs_levels(self):
        print "now testing find jobs with levels ..."
        index_job_levels(self.conn, "job1" ,[('q1', 1)])
        index_job_levels(self.conn, "job2", [('q1', 0), ('q2', 2)])

        self.assertEquals(search_job_levels(self.conn, [('q1', 0)]), [])
        self.assertEquals(search_job_levels(self.conn, [('q1', 1)]), ['job1'])
        self.assertEquals(search_job_levels(self.conn, [('q1', 2)]), ['job1'])
        self.assertEquals(search_job_levels(self.conn, [('q2', 1)]), [])
        self.assertEquals(search_job_levels(self.conn, [('q2', 2)]), [])
        self.assertEquals(search_job_levels(self.conn, [('q1', 0), ('q2', 1)]), [])
        self.assertEquals(search_job_levels(self.conn, [('q1', 0), ('q2', 2)]), ['job2'])
        self.assertEquals(search_job_levels(self.conn, [('q1', 1), ('q2', 1)]), ['job1'])
        self.assertEquals(search_job_levels(self.conn, [('q1', 1), ('q2', 2)]), ['job1', 'job2'])
        print "which passed"

    def test_index_and_find_jobs_years(self):
        print "now testing find jobs with years ..."
        index_job_years(self.conn, "job1",[('q1',1)])
        index_job_years(self.conn, "job2",[('q1',0),('q2',2)])

        self.assertEquals(search_job_years(self.conn, [('q1',0)]), [])
        self.assertEquals(search_job_years(self.conn, [('q1',1)]), ['job1'])
        self.assertEquals(search_job_years(self.conn, [('q1',2)]), ['job1'])
        self.assertEquals(search_job_years(self.conn, [('q2',1)]), [])
        self.assertEquals(search_job_years(self.conn, [('q2',2)]), [])
        self.assertEquals(search_job_years(self.conn, [('q1',0), ('q2', 1)]), [])
        self.assertEquals(search_job_years(self.conn, [('q1',0), ('q2', 2)]), ['job2'])
        self.assertEquals(search_job_years(self.conn, [('q1',1), ('q2', 1)]), ['job1'])
        self.assertEquals(search_job_years(self.conn, [('q1',1), ('q2', 2)]), ['job1','job2'])
        print "which passed"

if __name__ == '__main__':
    unittest.main()
