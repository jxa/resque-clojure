(ns resque-clojure.test.core
  (:refer-clojure :except [keys])
  (:use [resque-clojure.core])
  (:use [clojure.test])
  (:use [resque-clojure.redis]))

(def test-key "resque-clojure-test")
(def test-key-2 "resque-clojure-test-2")
(defn full-q [q] (str "resque:queue:" q))

(use-fixtures :each
              (fn [do-tests]
                nil
                (do-tests)
                (do
                  (del test-key)
                  (del test-key-2)
                  (del (full-q test-key))
                  (del (full-q test-key-2))
                  (del "resque:queues"))))

(deftest test-namespace-key
  (is (= "resque:key" (namespace-key "key"))))

(deftest test-full-queue-name 
  (is (= "resque:queue:test" (full-queue-name "test"))))

(deftest test-enqueue-dequeue
  (del (full-queue-name test-key))
  (is (nil? (dequeue [test-key])))
  (enqueue test-key "data")
  (is (some #{test-key} (smembers "resque:queues")))
  (is (= {:queue test-key :data {:class "data" :args nil}} (dequeue [test-key]))))

(deftest test-multiple-queues-pop-only-one
  (enqueue test-key "data")
  (enqueue test-key-2 "data2")
  (is (not (nil? (dequeue [test-key test-key-2]))))
  (is (or (and (= 1 (llen (full-q test-key))) (= 0 (llen (full-q test-key-2))))
          (and (= 0 (llen (full-q test-key))) (= 1 (llen (full-q test-key-2)))))))

(deftest test-format-error
  (let [e (try (/ 1 0) (catch Exception e e))
        job {:class "clojure.core/slurp" :args ["/etc/passwd"]}
        result {:exception e :job job :queue "test-queue"}
        formatted (format-error result)]
    (is (re-find #"^\d{4}/\d\d/\d\d \d{1,2}:\d\d:\d\d" (:failed_at formatted)))
    (is (= (job (:payload formatted))))
    (is (= "java.lang.ArithmeticException" (:exception formatted)))
    (is (= "Divide by zero" (:error formatted)))
    (is (re-find #"^clojure.lang.Numbers.divide" (first (:backtrace formatted))))
    (is (= "hostname:pid:queue" (:worker formatted)))
    (is (= "test-queue" (:queue formatted)))))