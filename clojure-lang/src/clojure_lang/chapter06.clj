(ns clojure-lang.chapter06
  (:require [taoensso.carmine :as car]
            [cheshire.core :as cc]
            [clj-time.coerce :as ctc]
            [clj-time.core :as ct]
            [clojure.string :refer [starts-with?]]
            [clojure-lang.util :refer [wcar* server-conn]]))


;;;;;;;;;;;;;;;;;;;;;;
;; 6.1 Autocomplete ;;
;;;;;;;;;;;;;;;;;;;;;;

;; Listing 6.1: The add_update_contact() function

(defn add-update-contact
  [user contact]
  (let [ac-list (str "recent:" user)]
    (wcar* (car/lrem ac-list 0 contact)
           (car/lpush ac-list contact)
           (car/ltrim ac-list 0 99))))


(defn remove-contact
  [user contact]
  (wcar* (car/lrem (str "recent:" user) contact)))


;; Listing 6.2: the fetch_autocomplete_list() function

(defn fetch-autocomplete-list
  [user prefix]
  (let [list-key (str "recent:" user)
        candidates (wcar* (car/lrange list-key 0 -1))]
    (filter (fn [candidate]
              (starts-with? candidate prefix))
            candidates)))


;; Listing 6.8: The acquire_lock() function

(defn acquire-lock
  [lockname & {:keys [acquire-timeout]
               :or {acquire-timeout 10}}]
  (let [identifier (str (java.util.UUID/randomUUID))
        end (+ (ctc/to-long (ct/now))
               acquire-timeout)]
    (loop []
      (cond
        (< end (ctc/to-long (ct/now))) false
        (= 1 (wcar* (car/setnx (str "lock:" lockname) identifier))) identifier
        :else (do (Thread/sleep 100)
                  (recur))))))


;; Listing 6.10: The release_lock() function

(defn release-lock
  [lockname identifier]
  (let [lock-key (str "lock:" lockname)]
    (wcar* (car/watch lock-key)
           (if (= identifier (car/get lock-key))
             (car/del lock-key)
             (car/unwatch)))))

;; Listing 6.9: The purchase_item_with_lock() function

(defn purchase-item-with-lock
  [buyer-id item-id seller-id list-price]
  (let [buyer-key (str "users:" buyer-id)
        seller-key (str "users:" seller-id)
        item-key (format "%s.%s" item-id seller-id)
        inventory-key (str "inventory:" buyer-id)
        price (wcar* (car/zscore "market:" item-key))
        funds (wcar* (car/hget buyer-key "funds"))
        lock-identifier (acquire-lock "market")]
    (when (and (= price list-price)
               (< price funds))
      (car/atomic server-conn
                  5
                  (car/watch "market:" buyer-key)
                  (car/multi)
                  (car/hincrby seller-key "funds" (Integer/parseInt price))
                  (car/hincrby buyer-key "funds" (- (Integer/parseInt price)))
                  (car/sadd inventory-key item-id)
                  (car/zrem "market:" item-id)))
    (when lock-identifier
      (release-lock "market" lock-identifier))))

;; Listing 6.11: The acquire_lock_with_timeout() function

(defn acquire-lock-with-timeout
  [lockname & {:keys [acquire-timeout lock-timeout]
               :or {acquire-timeout 10
                    lock-timeout 10}}]
  (let [identifier (str (java.util.UUID/randomUUID))
        end (+ (ctc/to-long (ct/now))
               acquire-timeout)]
    (loop []
      (cond
        (< end (ctc/to-long (ct/now)))
        false

        (= 1 (wcar* (car/setnx (str "lock:" lockname) identifier)))
        (do (wcar* (car/expire (str "lock:" lockname) lock-timeout))
            identifier)

        :else
        (do (Thread/sleep 100)
            (recur))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; 6.3 Counting Semaphores ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Listing 6.12: The acquire_semaphore() function

(defn acquire-semaphore
  [semname limit & {:keys [timeout]
                    :or {timeout 10}}]
  (let [identifier (str (java.util.UUID/randomUUID))
        now (ctc/to-long (ct/now))
        result (wcar* (car/zremrangebyscore semname Double/NEGATIVE_INFINITY (- now timeout))
                      (car/zadd semname now identifier)
                      (car/zrank semname identifier) limit)]
    (if (< (last result) limit)
      identifier
      (do
        (wcar* (car/zrem semname identifier))
        nil))))

;; Lisiting 6.13: The release_semaphore() function

(defn release-semaphore
  [semname identifier]
  (wcar* (car/zrem semname identifier)))

;; Listing 6.14: The acquire_fair_semaphore() function

(defn acquire-fair-semaphore
  [semname limit & {:keys [timeout]
                    :or {timeout 10}}]
  (let [identifier (str (java.util.UUID/randomUUID))
        czset (str semname ":owner")
        ctr (str semname ":counter")
        now (ctc/to-long (ct/now))
        counter-result (wcar* (car/zremrangebyscore semname Double/NEGATIVE_INFINITY (- now timeout))
                              (car/zinterstore czset 2 czset semname "weights" 1 0)
                              (car/incr ctr))
        counter (last counter-result)
        acquire-sem-result (wcar* (car/zadd semname now identifier)
                                  (car/zadd czset counter identifier)
                                  (car/zrank czset identifier))]
    (if (< (last acquire-sem-result) limit)
      identifier
      (do
        (wcar* (car/zrem semname identifier)
               (car/zrem czset identifier))
        nil))))

;; Listing 6.15: The release_fair_semaphore() function

(defn release-fair-semaphore
  [semname identifier]
  (-> (wcar* (car/zrem semname identifier)
             (car/zrem (str semname ":owner") identifier))
      first
      (= 1)))

;; Listing 6.16: The refresh_fair_semaphore() function

(defn refresh-fair-semaphore
  [semname identifier]
  (if (wcar* (car/zadd semname (ctc/to-long (ct/now)) identifier))
    (do
      (release-fair-semaphore semname identifier)
      false)
    true))

;; Listing 6.17: The acquire_semaphore_with_lock() function

(defn acquire-semaphore-with-lock
  [semname limit & {:keys [timeout]
                    :or {timeout 10}}]
  (let [identifier (acquire-lock semname :acquire-timeout 10)]
    (if identifier
      (try
        (acquire-fair-semaphore semname limit :timemout timeout)
        (finally
          (release-lock semname identifier))))))

;;;;;;;;;;;;;;;;;;;;;
;; 6.4 Task Queues ;;
;;;;;;;;;;;;;;;;;;;;;

;; Listing 6.18: The send_sold_email_via_queue() function

(defn send-sold-email-via-queue
  [seller item price buyer]
  (let [data {:seller_id seller
              :item_id item
              :price price
              :buyer_id buyer
              :time (ctc/to-long (ct/now))}]
    (wcar* (car/rpush "queue:email" (cc/generate-string data)))))

;; Listing 6.19: The process_sold_email_queue() function

(defn process-sold-email-queue
  []
  (let [packed (wcar* (car/blpop "queue:email" 30))]
    (if packed
      (println "Sending email with this content:"
               (cc/parse-string (last packed) true))
      (println "No item in queue."))))

;;;;;;;;;;;;;;;;;;;;;;;;
;; 6.5 Pull Messaging ;;
;;;;;;;;;;;;;;;;;;;;;;;;

;; Listing 6.25: The send_message() function

(defn send-message
  [chat-id sender message]
  (let [identifier (acquire-lock (str "chat:" chat-id))]
    (if (not identifier)
      (throw (Exception. "Couldn't get the lock"))
      (do
        (try
          (let [mid (wcar* (car/incr (str ":ds:" chat-id)))
                ts (ctc/to-long (ct/now))
                data (-> {:id mid :ts ts :sender sender :message message}
                         cc/generate-string)]
            (wcar* (car/zadd (str "msgs:" chat-id) mid data)))
          (finally
            (release-lock (str "chat:" chat-id) identifier)))
        chat-id))))

;; Listing 6.24: The create_chat() function

(defn create-chat
  [sender recipients message & {:keys [chat-id]}]
  (let [chat-id (or chat-id
                    (wcar* (car/incr "ids:chat")))
        create-chat! (fn [recipient-id]
                       (car/zadd (str "chat:" chat-id)
                                 0
                                 recipient-id)
                       (car/zadd (str "seen:" recipient-id)
                                 0
                                 chat-id))]
    (wcar* (doseq [recipient recipients]
             (create-chat! recipient)))
    (send-message chat-id sender message)))

;; Listing 6.26: The fecth_pending_messages() function

(defn fetch-pending-messages
  [recipient]
  (let [seen (wcar* (car/zrange (str "seen:" recipient) 0 -1 "withscores"))
        partitioned-seen (partition 2 seen)
        fetch-msgs (fn [[chat-id seen-id]]
                     (wcar* (car/zrangebyscore (str "msgs:" chat-id)
                                               (inc (Integer/parseInt seen-id))
                                               Double/POSITIVE_INFINITY)))
        chat-info (->> partitioned-seen
                       (mapv fetch-msgs)
                       (zipmap partitioned-seen))]
    (reduce-kv (fn [m [chat-id seen-id] v]
                 (when v
                   (let [parsed-messages (map #(cc/parse-string % true) v)
                         seen-id (:id (last parsed-messages))
                         _ (wcar* (car/zadd (str "chat:" chat-id) seen-id recipient))
                         min-id (wcar* (car/zrange (str "chat:" chat-id)
                                                   0
                                                   0
                                                   "withscores"))]
                     (wcar* (car/zadd (str "seen:" recipient) seen-id chat-id))
                     (when min-id
                       (wcar* (car/zremrangebyscore (str "msgs:" chat-id) 0 (last min-id))))
                     (assoc m chat-id parsed-messages))))
               {}
               chat-info)))


;; Listing 6.27: The join_chat() function

(defn join-chat
  [chat-id user]
  (let [message-id (-> (wcar* (car/get (str "ids:" chat-id)))
                       Integer/parseInt)]
    (wcar* (car/zadd (str "chat:" chat-id) message-id user)
           (car/zadd (str "seen:" user) message-id chat-id))))


;; Listing 6.28: The leave_chat() function

(defn leave-chat
  [chat-id user]
  (wcar* (car/zrem (str "chat:" chat-id) user)
         (car/zrem (str "seen:" user) chat-id)
         (if (not (car/zcard (str "chat:" chat-id)))
           (do
             (car/del (str "msgs:" chat-id))
             (car/del (str "ids:" chat-id)))
           (let [oldest (car/zrange (str "chat:" chat-id) 0 0 "withscores")]
             (car/zremrangebyscore (str "chat:" chat-id) 0 oldest)))))
