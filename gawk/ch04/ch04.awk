@load "redis"
BEGIN{
  c=connectRedis()
  select(c,12)
  testListItem(c, 0)
  testPurchaseItem(c)
  testBenchmarkUpdateToken(c)
}

function testListItem(c, nested) { 
  if (!nested){
     print "\n----- testListItem -----"
  }
  print "We need to set up just enough state so that a user can list an item"
  seller = "userX"
  item = "itemX"
  sadd(c,"inventory:"seller, item)
  delete(AR)
  smembers(c,"inventory:"seller,AR)
  print "The user's inventory has:"
  for(i in AR) {
    print "  "AR[i]
  }
  print 
  print "Listing the item..."
  l = listItem(conn, item, seller, 10) 
  print "Listing the item succeeded? "l
  delete(AR)
  zrangeWithScores(c,"market:",AR,0, -1)
  print "The market contains:"
  for(i=1;i<=length(AR);i+=2){
     print "  "AR[i]", "AR[i+1]
  }
}

function testPurchaseItem(c) {
   print "\n----- testPurchaseItem -----"
   testListItem(c, 1)
   print "We need to set up just enough state so a user can buy an item"
   hset(c,"users:userY", "funds", "125")
   delete(AR)
   hgetall(c,"users:userY",AR)
   print "The user has some money:"
   for(i=1;i<=length(AR);i+=2){
     print "  "AR[i]": "AR[i+1] 
   }
   print 
   print "Let's purchase an item"
   p = purchaseItem(c, "userY", "itemX", "userX", 10) 
   print "Purchasing an item succeeded? "p
   delete(AR)
   hgetall(c,"users:userY",AR)
   print "Their money is now:"
   for(i=1;i<=length(AR);i+=2){
     print "  "AR[i]": "AR[i+1] 
   }
   buyer = "userY"
   delete(AR)
   smembers(c,"inventory:"buyer,AR)
   print "Their inventory is now:"
   for(member in AR) {
     print "  "AR[member]
   }
}

function testBenchmarkUpdateToken(c) {
   print "\n----- testBenchmarkUpdate -----"
   benchmarkUpdateToken(c, 5)
}

function listItem(c, itemId, sellerId, price) {
   inventory = "inventory:"sellerId
   item = itemId"."sellerId
   now=systime()*1000
   end=now+5000
   while (now < end) {
     watch(c,inventory)
     if(!sismember(c,inventory, itemId)){
       unwatch(c)
       return 0
     }
     multi(c)
     zadd(c,"market:", price, item)
     srem(c,inventory, itemId)
     if(exec(c,R)==0) {
       now=systime()*1000
       continue
     }
     return 1
   }
   return 0
}
   
function purchaseItem(c, buyerId, itemId, sellerId, lprice) {
   buyer = "users:"buyerId
   seller = "users:"sellerId
   item = itemId"."sellerId
   inventory = "inventory:"buyerId
   end=(systime()*1000)+10000
   while ((systime()*1000) < end){
     watch(c,"market:"buyer)
     price = zscore(c,"market:", item)
     funds = hget(c,buyer, "funds")
     if (price != lprice || price > funds){
       unwatch()
       return 0
     }
     multi(c)
     hincrby(c, seller, "funds", price)
     hincrby(c, buyer, "funds", -price)
     sadd(c, inventory, itemId)
     zrem(c, "market:", item)
     if(exec(c,R)==0) {
        continue
     }
     return 1
   }
   return 0
}

function benchmarkUpdateToken(c, duration) {
  for(i=1;i<=2;i++) {
     if(i==1){
        method="updateToken"
     }
     else {
        method="updateTokenPipeline"
	p=pipeline(c)
     }
     count = 0
     start=systime()*1000
     end = start + (duration * 1000)
     while((systime()*1000) < end){
        count++;
	if(i==1){
          updateToken(c, "token", "user", "item")
        }
        else {
          updateTokenPipeline(c, "token", "user", "item", p)
        }
     }
     delta=(systime()*1000) - start
     print method" "count" "(delta / 1000)" "(count / (delta / 1000))
  }   
}

function updateToken(c, token, user, item) {
   timestamp=systime()
   hset(c,"login:", token, user)
   zadd(c,"recent:", timestamp, token)
   if (item) {
     zadd(c,"viewed:"token, timestamp, item)
     zremrangebyrank(c, "viewed:"token, 0, -26)
     zincrby(c, "viewed:", -1, item)
   }
}

function updateTokenPipeline(c, token, user, item, p) {
    timestamp=systime()
    hset(p,"login:", token, user)
    zadd(p,"recent:", timestamp, token)
    if (item) {
      zadd(p,"viewed:"token, timestamp, item)
      zremrangebyrank(p,"viewed:"token, 0, -26)
      zincrby(p,"viewed:", -1, item)
    }
    for(ERRNO="" ; ERRNO=="" ; getReply(p,REPLY)) 
      ;
}
