(defproject resque-clojure "0.3.0"
  :description "Redis based library for asynchronous processing"
  :url "https://github.com/jxa/resque-clojure"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/data.json "0.2.2"]
                 [redis.clients/jedis "2.1.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]]}})
