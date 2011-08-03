(ns resque-clojure.test.redis
  (:use [resque-clojure.redis]
        [clojure.test]))

(use-fixtures :once
              (fn [do-tests]
                (init {:host "localhost" :port 6379})
                (do-tests)
                (finalize)))

(deftest it-can-connect
  (is (not (nil? *pool*))))

