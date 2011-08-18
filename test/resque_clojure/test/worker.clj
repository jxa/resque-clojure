(ns resque-clojure.test.worker
  (:refer-clojure :exclude [name])
  (:use [resque-clojure.worker]
        [clojure.test]))

(deftest lookup-fn-test
  (is (= #'clojure.core/str (lookup-fn "clojure.core/str"))))

(defn exceptional [& args] (/ 1 0))

(deftest work-on-test
  (let [good-job {:func "clojure.core/str" :args ["foo"] :queue "test-queue"}
        bad-job {:func "resque-clojure.test.worker/exceptional" :args ["foo"]}]
    (is (= {:result :pass :job good-job :queue "test-queue"} (work-on "agent-state" good-job)))
    (is (= :error (:result (work-on "agent-state" bad-job))))
    (is (= java.lang.ArithmeticException (.getClass (:exception (work-on "agent-state" bad-job)))))))

