(ns atomic.test
    (:require atomic)
    (:use clojure.core)
    (:use clojure.test
          atomic))

(def schema (create-schema))

(deftable 
  schema :user
  :id
  :name)

(deftable 
  schema 
  :review
  :id
  :user_id
  :comment
  :rating)

(defn memory-db
  []
  (java.sql.DriverManager/getDrivers)
  (create-engine {:url "jdbc:sqlite::memory:"}))

(deftest 
  execute-test
  (let [engine (memory-db)
        result (execute engine "SELECT 1" [])
        rows (:rows result)]
    (is (= rows [[1]]))))

(deftest 
  build-result-test
  (is (=
        [{:a {:b "x" :c "y"} :q {:z "z"}}]
        (build-result [["x" "y" "z"]] [[:a :b] [:a :c] [:q :z]]))))

(defn init-drivers
  []
  (java.sql.DriverManager/getDrivers))

(deftest 
  simple-test
  (init-drivers)
  (let [engine (memory-db)]
    (execute engine "create table user (id integer primary key, name text, created_at integer)")
    (execute engine "create table review (id integer primary key, user_id integer, comment text)")
    (is (= []
         (:rows (execute engine "select id from user"))))
    (execute engine "insert into user (id, name) values (?, ?)" [1 "Brandon"])
    (execute engine "insert into user (id, name) values (?, ?)" [2 "Alice"])
    (execute engine "insert into user (id, name) values (?, ?)" [3 "Bob"])
    (is (= [[1 "Brandon"]]
         (:rows (execute engine "select id, name from user"))))
    (execute engine "insert into review (user_id, comment) values (?, ?)" [1 "liked it"])
    (execute engine "insert into review (user_id, comment) values (?, ?)" [2 "see you tomorrow"])
    (execute engine "insert into review (user_id, comment) values (?, ?)" [3 "cheesesteak"])
 
    (is (= 
            (-> 
              select
              (from :user {:as :u})  
              (join :review {:as :r} 
                    (on 
                      (= :r.user_id :u.id)))
              (where (= :u.id 2))
              (compile-query schema))
             []))
))
  
