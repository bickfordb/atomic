(ns atomic.test
  (:use atomic
        lg
        clojure.test))

;(lg/reset-channels!)
;(lg/add-channel! (lg/stdout-channel))
;(lg/set-level! 'atomic lg/DEBUG)

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


(def join-schema
  (new-schema
    (table :user
           (column :id)
           (column :name))
    (table :user_email
           (column :id)
           (column :user_id)
           (column :email))))

(defn init-join-sql
  []
  (exec-sql "create table user (
            id integer not null primary key,
            name text)")
  (exec-sql "create table user_email (
            id integer not null primary key,
            user_id integer not null,
            email text)")
  (INSERT join-schema :user {:name "Brandon" :id 1})
  (INSERT join-schema :user {:name "Doug" :id 2})
  (INSERT join-schema :user {:name "Sam" :id 3})
  (INSERT join-schema :user_email {:id 1
                                   :user_id 1
                                   :email "brandon@person"})
  (INSERT join-schema :user_email {:id 2
                                   :user_id 2
                                   :email "doug@person.com"}))

(dbtest join-test
        (init-join-sql)
        (let [rows (SELECT join-schema :user
                           (JOIN :user_email :e
                                 (ON (?= :id :e.id)))
                           (WHERE (?= :id 1)))]
          (is (= rows [{:id 1
                        :name "Brandon"
                        :e.email "brandon@person"
                        :e.id 1
                        :e.user_id 1}]))))

(dbtest left-join-test
        (init-join-sql)
        (let [rows (SELECT join-schema :user
                           (LEFT-JOIN :user_email :e
                                 (ON (?= :id :e.id)))
                           (WHERE (?= :e.id nil)))]
          (is (= rows [{:id 3
                        :name "Sam"
                        :e.email nil
                        :e.id nil
                        :e.user_id nil}]))))

