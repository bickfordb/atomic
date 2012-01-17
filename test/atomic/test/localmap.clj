(ns atomic.test.localmap
    (:require [atomic.localmap :as localmap])
    (:use clojure.test)) 

(deftest test-create (localmap/create))

(deftest test-put 
         (let [m (localmap/create)]
           (localmap/setdefault m "a" (fn [] "b"))
           (is (= (localmap/get m "a") "b"))))

