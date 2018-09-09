(ns clojure-lang.util
  (:require [taoensso.carmine :as car]))


(def server-conn
  {:pool {:max-total-per-key 16
          :max-idle-per-key 16
          :min-idle-per-key 12
          :max-wait-ms 5000
          :soft-min-evictable-idle-time-ms 60000
          :min-evictable-idle-time-ms -1}
   :spec {:host "localhost"
          :port 6379}})


(defmacro wcar*
  [& body]
  `(car/wcar server-conn ~@body))
