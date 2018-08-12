(ns clojure-lang.chapter02
  (:require [taoensso.carmine :as car]
            [clojure-lang.util :as clu]
            [clj-time.coerce :as ctc]
            [clj-time.core :as ct]
            [cheshire.core :as cc]))

;; 2.1 Login and Cookie Caching

(defn check-token
  [token]
  (clu/wcar* (car/hget "login:" token)))


(defn update-token
  [token user item]
  (let [timestamp (ctc/to-long (ct/now))]
    (clu/wcar* (car/hset "login:" token user))
    (clu/wcar* (car/zadd "recent:" timestamp token))
    (when item
      (clu/wcar* (car/zadd (str "viewed:" token) timestamp item))
      (clu/wcar* (car/zremrangebyrank (str "viewed:" token) 0 -26))
      ;; Added as per 2.5 section
      (clu/wcar* (car/zincrby "viewed:" -1 item)))))


(defn clean-sessions
  []
  (let [size (clu/wcar* (car/zcard "recent:"))]
    (when (> size 0)
      (let [end-index (min size 100)
            tokens (clu/wcar* (car/zrange "recent:" 0 (dec end-index)))]
        (doseq [token tokens]
          (clu/wcar* (car/del (str "viewed:" token)))
          (clu/wcar* (car/hdel "login:" token))
          (clu/wcar* (car/zrem "recent:" token)))))))


;; 2.2 Shopping carts in Redis

(defn add-to-cart
  [session item item-count]
  (if (<= item-count 0)
    (clu/wcar* (car/hdel (str "cart:" session) item))
    (clu/wcar* (car/hset (str "cart:" session) item item-count))))


(defn clean-full-ressions
  []
  (let [size (clu/wcar* (car/zcard "recent:"))]
    (when (> size 10000000)
      (let [end-index (min (- size 10000000) 100)
            tokens (clu/wcar* (car/zrange "recent:" 0 (dec end-index)))]
        (doseq [token tokens]
          (clu/wcar* (car/del (str "viewed:" token)))
          (clu/wcar* (car/hdel "login:" token))
          (clu/wcar* (car/zrem "recent:" token))
          (clu/wcar* (car/del (str "cart:" token))))))))


;; 2.3 Web Page Caching

(defn can-cache
  [request]
  (let [item-id (get request "item_id")]
    (when (seq item-id)
      (let [rank (clu/wcar* (car/zrank "viewed:" item-id))]
        (boolean (and rank
                      (< rank 10000)))))))


(defn cache-request
  [request callback]
  (if (not (can-cache request))
    (callback request)
    (let [page-key (hash request)
          content (clu/wcar* (car/get page-key))]
      (if (not (seq content))
        (let [content (callback request)]
          (clu/wcar* (car/setex page-key 300 content))
          content)
        content))))


;; 2.4 Database row caching

(defn schedule-row-cache
  [row-id delay-time]
  (clu/wcar* (car/zadd "delay:" delay-time row-id))
  (clu/wcar* (car/zadd "schedule:" (ctc/to-long (ct/now)) row-id)))



(defn get-inventory
  [row-id]
  {:title "Best product in the world"
   :price "RS100"
   :available-units 100})


(defn cache-rows
  []
  (let [next-schedule (clu/wcar* (car/zrange "schedule:" 0 0 "withscores"))
        time (ctc/to-long (ct/now))
        schedule-time (Long/parseLong (last next-schedule))]
    (when (and next-schedule
               (< schedule-time time))
      (let [row-id (first next-schedule)
            delay-time (Integer/parseInt (clu/wcar* (car/zscore "delay:" row-id)))]
        (when (<= delay-time 0)
          (clu/wcar* (car/zrem "delay:" row-id))
          (clu/wcar* (car/zrem "schedule:" row-id))
          (clu/wcar* (car/del (str "inv:" row-id))))
        (clu/wcar* (car/zadd "schedule:" (+ time delay-time) row-id))
        (clu/wcar* (car/set (str "inv:" row-id) (cc/generate-string (get-inventory row-id))))))))


;; 2.5 Web Page Analytics

(defn rescale-viewed
  []
  (clu/wcar* (car/zremrangebyrank "viewed:" 20000 -1))
  (clu/wcar* (car/zinterstore "viewed:" 1 "viewed:" "weights" 0.5)))
