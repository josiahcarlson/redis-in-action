(ns clojure-lang.chapter04
  (:require [taoensso.carmine :as car]
            [clj-time.coerce :as ctc]
            [clj-time.core :as ct]
            [clojure-lang.util :refer [server-conn wcar*]]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; 4.4 Redis Transaction ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Listing 4.5: The list-time() function
(defn list-item
  [item-id seller-id price]
  (let [inventory-key (str "inventory:" seller-id)
        item-key (format "%s.%s" item-id seller-id)]
    (when (= 1 (wcar* (car/sismember inventory-key item-id)))
      (car/atomic server-conn
                  5
                  (car/watch inventory-key)
                  (car/multi)
                  (car/zadd "market:" price item-id)
                  (car/srem inventory-key item-id)))))


;; Listing 4.6: The purchase_item() function
(defn purchase-item
  [buyer-id item-id seller-id list-price]
  (let [buyer-key (str "users:" buyer-id)
        seller-key (str "users:" seller-id)
        item-key (format "%s.%s" item-id seller-id)
        inventory-key (str "inventory:" buyer-id)
        price (wcar* (car/zscore "market:" item-key))
        funds (wcar* (car/hget buyer-key "funds"))]
    (when (and (= price list-price)
               (< price funds))
      (car/atomic server-conn
                  5
                  (car/watch "market:" buyer-key)
                  (car/multi)
                  (car/hincrby seller-key "funds" (Integer/parseInt price))
                  (car/hincrby buyer-key "funds" (- (Integer/parseInt price)))
                  (car/sadd inventory-key item-id)
                  (car/zrem "market:" item-id)))))


;; Listing 4.8: The update_token_pipeline() function
(defn update-token-pipeline
  [token user item]
  (let [ts (ctc/to-long (ct/now))]
    (wcar* :as-pipeline
           (car/hset "login:" token user)
           (car/zadd "recent:" token ts)
           (when item
             (car/zadd (str "viewed:" token) ts item)
             (car/zremrangebyrank (str "viewed:" token) 0 -26)
             (car/zincrby "viewed:" item -1)))))
