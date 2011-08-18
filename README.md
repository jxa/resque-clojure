# resque-clojure

Resque (pronounced like "rescue") is a Redis-backed library for creating background jobs,
placing those jobs on multiple queues, and processing them later. It was originally developed
for the ruby language. https://github.com/defunkt/resque

Resque-clojure is a clojure-based implementation of the same protocol. It aims to be
fully interoperable with the original project.

## Usage

    (ns my.test
      (:require [resque-clojure.core :as resque]))
    
    (resque/configure {:host "localhost" :port 6379})
    
    ;; creating a job
    (resque/enqueue "testqueue" "clojure.core/println" "hello" "resque")
    
    ;; listening for jobs
    (resque/start ["testqueue"])

## TODO

* publish to clojars
* add resque:stat:* keys
* add resque-status as an option/plugin

## License

Copyright (C) 2011 EdgeCase

Distributed under the Eclipse Public License, the same as Clojure.
