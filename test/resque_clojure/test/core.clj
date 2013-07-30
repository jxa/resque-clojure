(ns resque-clojure.test.core
  (:use [clojure.test]
        [resque-clojure.core])
  (:require [resque-clojure.redis :as redis]
            [resque-clojure.resque :as resque]
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
  (is (= {:host "clojure.org" :port 9999 :timeout 999
          :password "fungi" :database 0 :uri nil} @redis/config))
  (is (= {:namespace "resque" :error-handler nil} @resque/config))
  (is (= {:max-shutdown-wait 5000 :poll-interval 1000 :max-workers 4} @super/config)))

(deftest configuration-sets-namespace
  (configure {:namespace "staging"})
  (is (= "staging" (:namespace @resque/config)))
  (configure {:namespace "resque"}))
