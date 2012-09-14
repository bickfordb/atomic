(ns atomic.test
  (:use atomic
        lg
        clojure.test))

(lg/reset-channels!)
(lg/add-channel! (lg/stderr-channel))
(lg/set-level! 'atomic lg/DEBUG)

(def schema
  (new-schema
    (table :foo
      (column :id)
      (column :name))))

(defn mk-pool
  []
  (create-pool "jdbc:sqlite::memory:"
    :generated-keys? false))

(defmacro dbtest
  [n & body]
  `(deftest
     ~n
     (with-pool (mk-pool)
                (with-conn
                  (serializable
                    ~@body)))))

(dbtest serializable-conn-test
        (let [c0 atomic/*conn*]
          (serializable
            (let [c1 atomic/*conn*]
              (is (identical? c1 c0))))))

(dbtest tx-conn-test
        (let [c0 atomic/*conn*]
          (is (not (nil? c0)))
          (serializable
            (let [c1 atomic/*conn*]
              (is (not (nil? c1)))
              (tx
                (is (identical? c1 c0)))))))

(dbtest with-opt-conn-test
        (let [c0 atomic/*conn*]
          (is (not (nil? c0)))
          (with-opt-conn
            (let [c1 atomic/*conn*]
              (is (not (nil? c1)))
              (tx
                (is (identical? c1 c0)))))))

(dbtest select-test
        (tx
          (exec-sql "create table foo (id integer primary key, name text)")
          (exec-sql "select * from foo")
          (INSERT schema :foo {:name "Cheese"})
          (INSERT schema :foo {:name "Apple"})
          (is (= [{:id 1 :name "Cheese"}]
                (SELECT schema :foo (WHERE (?= :id 1)))))))

(def example-migrations
  [(migration
    :up (exec-sql "CREATE TABLE bar (id integer primary key, name text)")
    :down (exec-sql "DROP TABLE bar"))
   (migration
    :up (exec-sql "CREATE TABLE baz (id integer primary key, first_name text, last_name text)")
    :down (exec-sql "DROP TABLE baz"))
   ])

(dbtest migration-test
        (with-conn
          (run-all-up-migrations! example-migrations)
          (exec-sql "select * from bar")
          (exec-sql "select * from baz")
          (run-all-down-migrations! example-migrations)))
