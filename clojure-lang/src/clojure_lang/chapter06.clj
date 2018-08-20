(ns clojure-lang.chapter06
  (:require [taoensso.carmine :as car]
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
             (car/unwatch lock-key)))))

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
        (< end (ctc/to-long (ct/now))) false
        (= 1 (wcar* (car/setnx (str "lock:" lockname) identifier))) (do (wcar* (car/expire (str "lock:" lockname) lock-timeout))
                                                                        identifier)
        :else (do (Thread/sleep 100)
                  (recur))))))
