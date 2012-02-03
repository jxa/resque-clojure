(ns resque-clojure.test.core
  (:use [clojure.test]
        [resque-clojure.core])
  (:require [resque-clojure.redis :as redis]
            [resque-clojure.supervisor :as super]))

(deftest configuration
  (configure {:host "clojure.org"
              :port 9999
              :max-shutdown-wait 5000
              :poll-interval 1000
              :max-workers 4
              :bogus 'badkey
              :timeout 999
              :password "fungi"})
  (is (= {:host "clojure.org" :port 9999 :timeout 999 :password "fungi"} @redis/config))
  (is (= {:max-shutdown-wait 5000 :poll-interval 1000 :max-workers 4} @super/config)))