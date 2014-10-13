@load "redis"
BEGIN{
 ONE_WEEK_IN_SECONDS = 7*86400
 VOTE_SCORE = 432
 ARTICLES_PER_PAGE = 25
 c=connectRedis()
 select(c,12)
 articleId=postArticle(c, "username", "A title", "http://www.google.com") 
 print "We posted a new article with id: "articleId
 print "Its HASH looks like:"
 hgetall(c,"article:"articleId,RET)
 for(i=1;i<=length(RET);i+=2) {
    print "  "RET[i]": "RET[i+1]
 }
 print 
 articleVote(c, "other_user", "article:"articleId)
 votes = hget(c,"article:"articleId, "votes")
 print "We voted for the article, it now has votes: "votes
 print "The currently highest-scoring articles are:"
 getArticles(c, 1, articles) #  articles is an array
 dumparray(articles,"")
 ARR[1]="new-group"
 addGroups(c, articleId, ARR)
 print "We added the article to a new group, other articles include:"
 delete(articles)
 getGroupArticles(c, "new-group", 1, articles)
 dumparray(articles,"")
}

function getGroupArticles(c, group, page, articles) {
  return getGroupArticles1(c, group, page, "score:", articles)
}

function getGroupArticles1(c, group, page, order, articles) {
   key=order""group
   if(!exists(c,key)) {
     ARI[1]="group:"group
     ARI[2]=order
     zinterstore(c,key,ARI,"aggregate max")
     expire(c,key, 60)
   }
   getArticles1(c, page, key, articles)
}

function getArticles(c, page, articles) {
  getArticles1(conn, page, "score:", articles)
}

function getArticles1(c, page, order, articles) {
  start = (page - 1) * ARTICLES_PER_PAGE
  end = start + ARTICLES_PER_PAGE - 1
  delete(RET)
  zrevrange(c,order,RET,start,end)
  for(i in RET) {
    hgetall(c,RET[i],AR)
    for(j=1;j<=length(AR);j+=2) {
      articles[i][AR[j]]=AR[j+1]
    }
    articles[i]["id"]=RET[i]
  }
}

function addGroups(c, articleId, TOADD) {
  article = "article:"articleId
  for(i in TOADD) {
     sadd(c,"group:"TOADD[i], article)
  }
}

function postArticle(c, user,title,link) {
  articleId=incr(c,"article:")
  voted="voted:"articleId;
  sadd(c,voted,user)
  expire(c,voted,ONE_WEEK_IN_SECONDS)
  now=systime()
  article = "article:"articleId
  AR[1]="title"
  AR[2]=title
  AR[3]="link"
  AR[4]="http://www.google.com"
  AR[5]="user"
  AR[6]=user
  AR[7]="now"
  AR[8]=now
  AR[9]="votes"
  AR[10]=1
  hmset(c,article,AR)
  zadd(c,"score:",now + VOTE_SCORE,article)
  zadd(c,"time:",now,article)
  return articleId
}

function articleVote(c, user, article) {
 cutoff= systime() - ONE_WEEK_IN_SECONDS
 if(zscore(c,"time:",article) < cutoff){
   return
 }
 articleId = substr(article,index(article,":")+1)
 if (sadd(c,"voted:"articleId,user) == 1) {
   zincrby(c,"score:", VOTE_SCORE, article)
   hincrby(c,article, "votes", 1)
 }
}

function dumparray(array,e,     i)
{
    for (i in array){
        if (isarray(array[i])){
	    print "  id: "array[i]["id"] 
            dumparray(array[i],"")
        }
        else {
	    if(e){
              printf("%s[%s] = %s\n",e,i, array[i])
            }
	    else {
	     if(i=="id")
	       continue
	     else  
               printf("    %s = %s\n",i, array[i])
            }
        }
     }
}
