(ns resque-clojure.test.worker
  (:use [resque-clojure.worker]
        [clojure.test]))

(deftest lookup-fn-test
  (is (= #'clojure.core/str (lookup-fn "clojure.core/str"))))

(deftest work-on-test
  (let [job {:class "clojure.core/str" :args ["foo"]}]
    (is (= {:result :pass :job job} (work-on "agent-state" job)))))

