(ns atomic.test
  (:require atomic)
  (:require [atomic.graph :as graph])
  (:use atomic.util)
  (:use clojure.core)
  (:use clojure.test
        atomic))

(def schema (create-schema))

(add-table 
  schema 
  :user
  :id
  :name)

(add-table 
  schema 
  :review
  :id
  :user_id
  :comment
  (has-one :reviewer :user :user_id :reviews))

(defn memory-db
  []
  (java.sql.DriverManager/getDrivers)
  (create-engine schema {:url "jdbc:sqlite::memory:"}))

(deftest 
  execute-sql-test
  (let [engine (memory-db)
        result (execute-sql engine "SELECT 1" [])
        rows (:rows result)]
    (is (= rows [[1]]))))

(deftest 
  build-result-test-nested
  (is (=
        [{:a {:b "x" :c "y"} :q {:z "z"}}]
        (build-result [["x" "y" "z"]] [[:a :b] [:a :c] [:q :z]]))))

(deftest 
  build-result-test-simple
  (is (=
        [{:a "x" :b "y" :c "z"}]
        (build-result [["x" "y" "z"]] [[:a] [:b] [:c]]))))

(defn init-drivers
  []
  (java.sql.DriverManager/getDrivers))

(deftest 
  get-in-test 
  (is
    (= 
      (get-in2 [:x] {})
      []))
  (is
    (= 
      (get-in2 [:x] {:x 1}) 
      [1]))
  (is
    (= 
      (get-in2 [] {:x 1}) 
      [{:x 1}]))
  (is
    (= 
      (get-in2 [:x :y] {:x [{:y 2}]}) 
      [2]))
  (is
    (= 
      (get-in2 [:x :y] {:x {:y 2}}) 
      [2]))
)
 
(deftest 
  each-in-test 
  (let [c (atom nil)]
    (each-in #(reset! c %) [:x :a] {:x {:a 1}})
    (is (= @c 1)))

  ; sequences
  (let [c (atom [])]
    (each-in #(swap! c conj %) [:x :a] [{:x {:a 1}} {:x {:a 2}}])
    (is (= @c [1, 2]))))

(deftest 
  map-in-test 
  (is (= [{:x 1} {:y 3 :x 1}] 
         (map-in #(assoc % :x 1) [] [{} {:y 3}])))
  
  (is (= {:y {:x 2 :q 1}}
         (map-in #(assoc % :q 1) [:y] {:y {:x 2 :q 1}})))

  (is (= {:q 1}
         (map-in #(assoc % :q 1) [] {}))))
       
(deftest 
  simple-test
  (init-drivers)
  (let [engine (memory-db)]
    (execute-sql engine "create table user (id integer primary key, name text, created_at integer)")
    (execute-sql engine "create table review (id integer primary key, user_id integer, comment text)")
    (is (= []
           (:rows (execute-sql engine "select id from user"))))
    (execute-sql engine "insert into user (id, name) values (?, ?)" [1 "Brandon"])
    (execute-sql engine "insert into user (id, name) values (?, ?)" [2 "Alice"])
    (execute-sql engine "insert into user (id, name) values (?, ?)" [3 "Bob"])
    (is (= [[1 "Brandon"]]
           (:rows (execute-sql engine "select id, name from user where id = ?" [1]))))
    (execute-sql engine "insert into review (id, user_id, comment) values (?, ?, ?)" [99 1 "liked it"])
    (execute-sql engine "insert into review (id, user_id, comment) values (?, ?, ?)" [100 2 "see you tomorrow"])
    (execute-sql engine "insert into review (id, user_id, comment) values (?, ?, ?)" [101 3 "cheesesteak"])
    (is (= 
          (-> 
            select
            (from :user {:as :u})  
            (join :review {:as :r} 
                  (on (= :r.user_id :u.id)))
            (where (= :u.id 2))
            (execute engine))
          []))

    (let [a-user (graph/one engine :user :reviews (= :id 1))]
      (println "user" a-user)
      (is (= 
            (:comment (first (:reviews a-user)))
            "liked it")))
    (let [a-review (graph/one engine :review :reviewer (= :user_id 1))]
      (println "a-review" a-review)
      (is (= 
            (:name (:reviewer a-review))
            "Brandon")))
  ))

(deftest parse-relation-paths-test
         (is (= 
               (parse-dotted-keyword :a)
               [:a]))
         (is (= 
               (parse-dotted-keyword :a.b)
               [:a :b]))
         (is (= 
               (parse-dotted-keyword :a.b.c)
               [:a :b :c])))

(deftest parse-relation-paths-test
         (is (=
               (parse-relation-paths [:a])
               #{[:a]}))
         (is (=
               (parse-relation-paths [:a :b])
               #{[:a] [:b]}))
         (is (=
               (parse-relation-paths [:a.b])
               #{[:a] [:a :b]})))

