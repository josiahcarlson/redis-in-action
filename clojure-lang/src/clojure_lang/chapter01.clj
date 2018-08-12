(ns clojure-lang.chapter01
  (:require [clojure.string :refer [split]]
            [cheshire.core :as cc]
            [taoensso.carmine :as car]
            [clj-time.coerce :as ctc]
            [clj-time.core :as ct]
            [clojure-lang.util :refer [wcar*]]))


(def ONE-WEEK-IN-SECONDS (* 7 86400))
(def VOTE-SCORE 432)
(def ARTICLES-PER-PAGE 25)

;; 1.3.1 Voting an articles

(defn article-vote
  [user article]
  (let [cutoff (- (ctc/to-long (ct/now))
                  ONE-WEEK-IN-SECONDS)
        last-vote-time (Long/parseLong (wcar* (car/zscore "time:" article)))]
    (when (or (not last-vote-time)
              (> last-vote-time cutoff))
      (let [article-id (last (split article #":"))]
        (when (wcar* (car/sadd (str "voted:" article-id) user))
          (wcar* (car/zincrby "score:" VOTE-SCORE article))
          (wcar* (car/hincrby article "votes" 1)))))))


;; 1.3.2 Posting and fetching articles

(defn post-article
  [user title link]
  (let [article-id (wcar* (car/incr "article:"))
        voted (str "voted:" article-id)
        time (ctc/to-long (ct/now))
        article (str "article:" article-id)
        article-object {:title title
                        :link link
                        :poster user
                        :time time
                        :votes 1}]
    (wcar* (car/sadd voted user))
    (wcar* (car/expire voted ONE-WEEK-IN-SECONDS))
    (wcar* (car/hmset article :title title :link link :poster user :time time :votes 1))
    (wcar* (car/zadd "score:" (+ time VOTE-SCORE) article))
    (wcar* (car/zadd "time:" time article))
    article))


(defn get-articles
  ([page]
   (get-articles page "score:"))
  ([page order]
   (let [start (* ARTICLES-PER-PAGE
                  (dec page))
         end (dec (+ start ARTICLES-PER-PAGE))
         ids (wcar* (car/zrevrange order start end))]
     (mapv (fn [id]
             (->> (wcar* (car/hgetall id))
                  (into ["id" id])
                  (apply array-map)))
           ids))))


;; 1.3.3 Grouping Articles

(defn add-remove-groups
  [article-id to-add to-remove]
  (let [article (str "article:" article-id)]
    (doseq [group to-add]
      (wcar* (car/sadd (str "group:" group) article)))
    (doseq [group to-remove]
      (wcar* (car/srem (str "group:" group) article)))))


(defn get-group-articles
  ([group page]
   (get-group-articles group page "score:"))
  ([group page order]
   (let [grouped-key (str order group)]
     (when-not (wcar* (car/exists grouped-key))
       (wcar* (car/zinterstore* grouped-key [(str "group:" + group) order] "max"))
       (wcar* (car/expire grouped-key 60)))
     (get-articles page grouped-key))))
