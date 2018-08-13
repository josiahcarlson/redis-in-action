(ns clojure-lang.chapter03
  (:require [taoensso.carmine :as car]
            [clojure-lang.util :refer [wcar* server-conn]]))

;;;;;;;;;;;;;;;;;
;; 3.1 Strings ;;
;;;;;;;;;;;;;;;;;

;; Listing 3.1: A sample interaction showing INCR and DECR operations in Redis
(wcar* (car/get "key"))
;; => nil
(wcar* (car/incr "key"))
;; => 1
(wcar* (car/incrby "key" 15))
;; => 16
(wcar* (car/decrby "key" 5))
;; => 11
(wcar* (car/get "key"))
;; => "11"
(wcar* (car/set "key" "13"))
;; => "OK"
(wcar* (car/incr "key"))
;; => 14

;; Listing 3.2: A sample interaction showing substring and bit operations in Redis
(wcar* (car/append "new-string-key" "hello "))
;; => 6
(wcar* (car/append "new-string-key" "world!"))
;; => 12
(wcar* (car/getrange "new-string-key" 3 7))
;; => "lo wo"
(wcar* (car/setrange "new-string-key" 0 "H"))
;; => 12
(wcar* (car/setrange "new-string-key" 6 "W"))
;; => 12
(wcar* (car/get "new-string-key"))
;; => "Hello World!"
(wcar* (car/setrange "new-string-key"11 ", how are you?"))
;; => 25
(wcar* (car/get "new-string-key"))
;; => "Hello World, how are you?"
(wcar* (car/setbit "another" 7 1))
;; => 0
(wcar* (car/get "another"))
;; => "!"

;;;;;;;;;;;;;;;
;; 3.2 Lists ;;
;;;;;;;;;;;;;;;

;; Listing 3.3: A sample interaction showing LIST push and pop commands in Redis
(wcar* (car/rpush "list-key" "last"))
;; => 1
(wcar* (car/lpush "list-key" "first"))
;; => 2
(wcar* (car/rpush "list-key" "new last"))
;; => 3
(wcar* (car/lrange "list-key" 0 -1))
;; => ["first" "last" "new last"]
(wcar* (car/lpop "list-key"))
;; => "first"
(wcar* (car/lpop "list-key"))
;; => "last"
(wcar* (car/lrange "list-key" 0 -1))
;; => ["new last"]
(wcar* (car/rpush "list-key" "a" "b" "c"))
;; => 4
(wcar* (car/lrange "list-key" 0 -1))
;; => ["new last" "a" "b" "c"]
(wcar* (car/ltrim "list-key" 2 -1))
;; => "OK"
(wcar* (car/lrange "list-key" 0 -1))
;; => ["b" "c"]

;; Listing 3.4:  Blocking LIST pop and movement commands in Redis
(wcar* (car/rpush "list" "item1"))
;; => 1
(wcar* (car/rpush "list" "item2"))
;; => 2
(wcar* (car/rpush "list2" "item3"))
;; => 1
(wcar* (car/brpoplpush "list2" "list" 1))
;; => "item3"
(wcar* (car/brpoplpush "list2" "list" 1))
;; => nil
(wcar* (car/lrange "list" 0 -1))
;; => ["item3" "item1" "item2"]
(wcar* (car/brpoplpush "list" "list2" 1))
;; => "item2"
(wcar* (car/blpop "list" "list2" 1))
;; => ["list" "item3"]
(wcar* (car/blpop "list" "list2" 1))
;; => ["list" "item1"]
(wcar* (car/blpop "list" "list2" 1))
;; => ["list2" "item2"]
(wcar* (car/blpop "list" "list2" 1))
;; => nil

;;;;;;;;;;;;;;
;; 3.3 Sets ;;
;;;;;;;;;;;;;;

;; Listing 3.5: A sample interaction showing some common SET commands in Redis
(wcar* (car/sadd "set-key" "a" "b" "c"))
;; => 3
(wcar* (car/srem "set-key" "c" "d"))
;; => 1
(wcar* (car/srem "set-key" "c" "d"))
;; => 0
(wcar* (car/scard "set-key"))
;; => 2
(wcar* (car/smembers "set-key"))
;; => ["a" "b"]
(wcar* (car/smove "set-key" "set-key2" "a"))
;; => 1
(wcar* (car/smove "set-key" "set-key2" "c"))
;; => 0
(wcar* (car/smembers "set-key2"))
;; => ["a"]

;; Listing 3.6: A sample interaction showing SET difference, intersection, and union in Redis

(wcar* (car/sadd "skey1" "a" "b" "c" "d"))
;; => 4
(wcar* (car/sadd "skey2" "c" "d" "e" "f"))
;; => 4
(wcar* (car/sdiff "skey1" "skey2"))
;; => ["a" "b"]
(wcar* (car/sinter "skey1" "skey2"))
;; => ["d" "c"]
(wcar* (car/sunion "skey1" "skey2"))
;; => ["c" "f" "a" "d" "b" "e"]

;;;;;;;;;;;;;;;;
;; 3.4 Hashes ;;
;;;;;;;;;;;;;;;;

;; Listing 3.7: A sample interaction showing some common HASH commands in Redis

(wcar* (car/hmset "hash-key" "k1" "v1" "k2" "v2" "k3" "v3"))
;; => "OK"
(wcar* (car/hmget "hash-key" "k2" "k3"))
;; => ["v2" "v3"]
(wcar* (car/hlen "hash-key"))
;; => 3
(wcar* (car/hdel "hash-key" "k1" "k3"))
;; => 2

;; Listing 3.8: A sample interaction showing some more advanced features of Redis HASHes

(wcar* (car/hmset "hash-key2" "short" "hello" "long" (apply str (take 1000 (repeat "1")))))
;; => "OK"
(wcar* (car/hkeys "hash-key2"))
;; => ["long" "short"]
(wcar* (car/hexists "hash-key2" "num"))
;; => 0
(wcar* (car/hincrby "hash-key2" "num" 1))
;; => 1
(wcar* (car/hexists "hash-key2" "num"))
;; => 1

;;;;;;;;;;;;;;;;;;;;;
;; 3.5 Sorted sets ;;
;;;;;;;;;;;;;;;;;;;;;

;; Listing 3.9: A sample interaction showing some common ZSET commands in Redis

(wcar* (car/zadd "zset-key" 3 "a" 2 "b" 1 "c"))
;; => 3
(wcar* (car/zcard "zset-key"))
;; => 3
(wcar* (car/zincrby "zset-key" 3 "c"))
;; => "4"
(wcar* (car/zscore "zset-key" "b"))
;; => "2"
(wcar* (car/zrank "zset-key" "c"))
;; => 2
(wcar* (car/zcount "zset-key" 0 3))
;; => 2
(wcar* (car/zrem "zset-key" "b"))
;; => 1
(wcar* (car/zrange "zset-key" 0 -1 "withscores"))
;; => ["a" "3" "c" "4"]

;; Listing 3.10: A sample interaction showing ZINTERSTORE and ZUNIONSTORE

(wcar* (car/zadd "zset-1" 1 "a" 2 "b" 3 "c"))
;; => 3
(wcar* (car/zadd "zset-2" 4 "b" 1 "c" 0 "d"))
;; => 3
(wcar* (car/zinterstore "zset-i" 2 "zset-1" "zset-2"))
;; => 2
(wcar* (car/zrange "zset-i" 0 -1 "withscores"))
;; => ["c" "4" "b" "6"]
(wcar* (car/zunionstore "zset-u" 2 "zset-1" "zset-2" "AGGREGATE" "MIN"))
;; => 4
(wcar* (car/zrange "zset-u" 0 -1 "withscores"))
;; => ["d" "0" "a" "1" "c" "1" "b" "2"]
(wcar* (car/sadd "set-1" "a" "d"))
;; => 2
(wcar* (car/zunionstore "zset-u2" 3 "zset-1" "zset-2" "set-1"))
;; => 4
(wcar* (car/zrange "zset-u2" 0 -1 "withscores"))
;; => ["d" "1" "a" "2" "c" "4" "b" "6"]

;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; 3.6 Publish/Subscribe ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Listing 3.11: Using PUBLISH and SUBSCRIBE in Redis

(def listener
  (car/with-new-pubsub-listener (:spec server-conn)
    {"channel" (fn [msg] (println msg))}
    (car/subscribe "channel")))


(defn publish
  [msg]
  (wcar* (car/publish "channel" msg)))


;; (car/close-listener listener)

;;;;;;;;;;;;;;;;;;;;;;;;
;; 3.7 Other Commands ;;
;;;;;;;;;;;;;;;;;;;;;;;;

;; Listing 3.12: A sample interaction showing some uses of SORT

(wcar* (car/rpush "sort-input" 23 15 110 7))
;; => 4
(wcar* (car/sort "sort-input"))
;; => ["7" "15" "23" "110"]
(wcar* (car/sort "sort-input" "ALPHA"))
;; => ["110" "15" "23" "7"]
(wcar* (car/hset "d-7" "field" 5))
;; => 1
(wcar* (car/hset "d-15" "field" 1))
;; => 1
(wcar* (car/hset "d-23" "field" 9))
;; => 1
(wcar* (car/hset "d-110" "field" 3))
;; => 1
(wcar* (car/sort "sort-input" "BY" "d-*->field"))
;; => ["15" "110" "7" "23"]
(wcar* (car/sort "sort-input" "BY" "d-*->field" "GET" "d-*->field"))
;; => ["1" "3" "5" "9"]


;; Listing 3.14: What can happen with transactions during parallel execution

(car/atomic server-conn
            1
            (car/multi)
            (car/incr "trans:")
            (car/incrby "trans:" -1))
;; => [["OK" "QUEUED" "QUEUED"] [1 0]]

;; Listing 3.15: A sample interaction showing the use of expiration-related commands in Redis

(wcar* (car/set "key" "value"))
;; => "OK"
(wcar* (car/get "key"))
;; => "value"
(wcar* (car/expire "key" 2))
;; => 1
(wcar* (car/get "key"))
;; => nil
(wcar* (car/set "key" "value"))
;; => "OK"
(wcar* (car/expire "key" 100))
;; => 1
(wcar* (car/ttl "key"))
;; => 87
