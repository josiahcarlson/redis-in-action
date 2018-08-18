(ns clojure-lang.chapter05
  (:require [taoensso.carmine :as car]
            [clj-time.coerce :as ctc]
            [clj-time.core :as ct]
            [clojure-lang.util :refer [wcar*]]
            [cheshire.core :as cc]))

;;;;;;;;;;;;;;;;;;;;;;;;;;
;; 5.1 Logging to Redis ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Listing 5.1: The log_recent() function
(def SEVERITY
  {"logging.DEBUG" "debug"
   "logging.INFO" "info"
   "logging.WARNING" "warning"
   "logging.CRITICAL" "critical"
   "logging.ERROR" "error"})

(defn log-recent
  [log-name message & {:keys [severity]
                       :or {severity "logging.INFO"}}]
  (let [severity (get SEVERITY severity)
        destination-key (format "recent:%s:%s" log-name severity)
        message (format "%s %s" (ctc/to-string (ct/now)) message)]
    (wcar* (car/lpush destination-key message)
           (car/ltrim destination-key 0 99))))


;; Listing 5.2: The log_common() function
(defn log-common
  [log-name message & {:keys [severity]
                       :or {severity "logging.INFO"}}]
  (let [severity-val (get SEVERITY severity)
        destination-key (format "common:%s:%s" log-name severity-val)
        start-key (str destination-key ":start")
        message* (format "%s %s" (ctc/to-string (ct/now)) message)]
    (wcar* (when (car/get start-key)
             (car/rename destination-key (str destination-key ":last"))
             (car/rename start-key (str destination-key ":pstart")))
           (car/zincrby destination-key 1 message*))
    (log-recent log-name message :severity severity)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; 5.2 Counter and Statistics ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; Listing 5.3: the update_counter() function
(def precision [1 5 60 300 3600])
(defn update-counter
  [key-name & {:keys [incrby now]
               :or {incrby 1}}]
  (let [now (or now (ctc/to-long (ct/now)))]
    (doseq [prec precision]
      (let [pnow (* (quot now prec) prec)
            hash-key (format "%s:%s" prec key-name)]
        (wcar* (car/zadd "known:" hash-key 0)
               (car/hincrby (str "count:" hash-key) pnow incrby))))))


(defn get-counter
  [key-name prec]
  (let [hash-key (format "%s:%s" prec key-name)
        data (wcar* (car/hgetall (str "count:" hash-key)))]
    (apply array-map data)))


;; Listing 5.6: The update_stats() function
(defn update-stats
  [context key-type value]
  (let [destination-key (format "stats:%s:%s" context key-type)
        start-key (str destination-key ":start")
        tkey1 (str (java.util.UUID/randomUUID))
        tkey2 (str (java.util.UUID/randomUUID))]
    (wcar* (when (car/get start-key)
             (car/rename destination-key (str destination-key ":last"))
             (car/rename start-key (str destination-key ":pstart"))
             (car/set start-key (ct/hour (ct/now))))
           (car/zadd tkey1 value "min")
           (car/zadd tkey2 value "max")
           (car/zunionstore destination-key 2 destination-key tkey1 "AGGREGATE" "MIN")
           (car/zunionstore destination-key 2 destination-key tkey2 "AGGREGATE" "MAX")
           (car/del tkey1 tkey2)
           (car/zincrby destination-key 1 "count")
           (car/zincrby destination-key value "sum")
           (car/zincrby destination-key (* value value) "sumsq"))))


;; TODO Add stddev calculation logic
;; Listing 5.7: The get_stats() function
(defn get-stats
  [context key-type]
  (let [stats-key (format "stats:%s:%s" context key-type)
        data (->> (wcar* (car/zrange stats-key 0 -1 "withscores"))
                  (apply array-map)
                  (reduce-kv (fn [m k v]
                               (assoc m k (Integer/parseInt v)))
                             {}))
        average (quot (get data "sum") (get data "count"))]
    (assoc data
           "average" average)))


;; Listing 5.8: The access_time() context manager
(defn with-access-time
  [handler]
  (fn [req]
    (let [start-ts (ctc/to-long (ct/now))
          context (get req "path")
          return-val (handler req)
          delta (- (ctc/to-long (ct/now))
                   start-ts)
          stats (update-stats context "AccessTime" delta)
          stats-sum (Integer/parseInt (first (drop 1 (reverse stats))))
          stats-counts (Integer/parseInt (first (drop 2 (reverse stats))))
          average (quot stats-sum stats-counts)]
      (wcar* (car/zadd "slowest:AccessTime" average context)
             (car/zremrangebyrank "slowest:AccessTime" 0 -101))
      return-val)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; 5.3 IP-to-City and Country Lookup ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; 5.4 Service discovery and configuration ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; Listing 5.13: The is_under_maintence() function
(defn is-under-maintenance
  []
  (-> (wcar* (car/get "is_under_maintenance"))
      boolean))


;; Listing 5.14: The set-config() function
(defn set-config
  [config-type component config]
  (let [config-key (format "config:%s:%s" config-type component)]
    (wcar* (car/set config-key (cc/generate-string config)))))


;; Listing 5.15: The get-config() function
(defn get-config
  [config-type component]
  (let [config-key (format "config:%s:%s" config-type component)]
    (-> (wcar* (car/get config-key))
        (cc/parse-string true))))
