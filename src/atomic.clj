(ns atomic
  "Library to simplify interaction with SQL databases

  Example:

    (use 'atomic)
    (def schema (create-schema))

    ; Describe a user table 
    (deftable 
      :user
      :id
      :name
      (has-many :emails :email :user_id :user))

    (deftable 
      :email
      :id
      :address
      :user_id)

    (def db (create-db \"org.sqlite.JDBC\" \"jdbc:sqlite::memory:\"))
    (execute-sql db \"create table user (id integer primary key, name text, created_at integer)\")
    (execute-sql db \"create table email (id integer primary key, user_id integer, address text)\")

    (insert db :user {:id 1 :name \"Brandon\"})
    (insert db :email {:address \"foo@bar.com\" :user_id 1})
    (insert db :email {:address \"bar@bar.com\" :user_id 1})

    (println (-> select 
                 (from :user)
                 (join :email (on (= :email.user_id :user.id)))
                 (where (= :id 1))
                 (execute db)))

    ; [{:user {:name \"Brandon\" :id 5} :email {:address \"foo@bar.com\" :user_id 1 :id 1}}
    ;  {:user {:name \"Brandon\" :id 5} :email {:address \"bar@bar.com\" :user_id 1 :id 2}}]

    ; Easy-join graph API (one/many):
    ; Get the \"Brandon\" record, and join in the related emails

    (println (one db :user :emails (= (:name \"Brandon\")))) 
    ; [{:name \"Brandon\" :id 5 :emails [{:address \"foo@bar.com\" :user_id 1 :id 1} {:address \"bar@bar.com\" :user_id 1 :id 2}]}]


  "
  (:refer-clojure :exclude [compile])
  (:use atomic.util)
  (:require [atomic.localmap :as localmap]
            clojure.string
            clojure.walk))

(defn create-schema
  "Create a schema object.

  Schema objects store table and foreign key definitions.  For simple
  applications this doesn't need to be called directly, usually you
  can use the default global schema object.  This is useful if you need to
  maintain multiple, conflicting table definitions in one
  application. 

  Example:
    ; Create a schema object:
    (def my-schema (create-schema))

    ; Use my-schema in :my_table's definition:
    (deftable :my_table {:schema my-schema}))
  "
  []
  {:tables (atom {})
   :foreign-keys (atom {})})

(def +schema+ (create-schema))

(defn create-db 
  "Create a new db instance.
  
  A db is a handle to a schema and a database URL.  

  Calls to (execute db... ) and (execute-sql db) will lazily create and
  maintain thread-local connections 
  "
  [driver url & opts] 
  {
   :driver driver
   :url url
   :schema (get opts :schema +schema+)
   :local (localmap/create)})

(defonce +jdbc-loaded+ (atom #{}))

(defn- load-jdbc-driver
  [driver-path]
  (when (not (contains? @+jdbc-loaded+ driver-path))
    (Class/forName driver-path)
    (swap! +jdbc-loaded+ conj driver-path)))

(defn- add-foreign-key
  [schema table-key fk]
  (swap! (schema :foreign-keys) assoc [table-key (:key fk)] fk))

(defn has-one 
  "Get a has-one foreign key
 
  This is used in a deftable call.

  For example:

    (deftable :user 
      (column :id)
      (column :name)
      (column :company_id)
      (has-one :employer :company :company_id :employees))

  Arguments
  key -- keyword, the name of the relation
  table -- keyword, the name of the destination table
  column -- keyword, the column name in the source table
  reverse -- keyword, the name of the reverse relation

  Returns
  A map with :type :has-one set
  "
  ([key table column] (has-one key table column nil))
  ([key table column reverse]
    {:type :has-one
     :key key
     :column column
     :table table
     :reverse reverse}))

(defn has-many 
  "Get a has-many foreign key
 
  This is used in a deftable call.

  For example:

    (deftable :user 
      (column :id)
      (column :name)
      (column :company_id))

   (deftable :company
      (column :id)
      (column :title)
      (has-many :employees :user :company_id :employer))

  Arguments
  key -- keyword, the name of the relation
  table -- keyword, the name of the destination table
  column -- keyword, the column name in the destination table
  reverse -- keyword, the name of the reverse relation

  Returns
  A map with :type :has-many set 
  "
  ([key table column] 
   (has-many key table column nil))
  ([key table column reverse]
     {:type :has-many
      :key key
      :column column
      :table table
      :reverse reverse}))

(defn- create-connection
  "Create a connection for an db"
  [db]
  (load-jdbc-driver (:driver db))
  (java.sql.DriverManager/getConnection ^String (:url db)))

(defn- get-connection
  "Get a connection from an db"
  [db]
  (let [m (:local db)]
    (localmap/setdefault m :connection (fn [] (create-connection db)))))

(defn- resultset-seq2
  "Creates and returns a vector of tuples corresponding to
  the rows in the java.sql.ResultSet rs"
  {:added "1.0"}
  [^java.sql.ResultSet rs]
    (let [n (inc (.getColumnCount (.getMetaData rs)))
          row-values (fn []
                       (let [row (transient (vector))]
                         (loop [i 1]
                            (when (< i n) 
                             (conj! row (.getObject rs i))
                             (recur (inc i))))
                         (persistent! row)))
          result (transient (vector))]
      (loop []
        (when (.next rs)
          (conj! result (row-values))
          (recur)))
      (persistent! result))) 

(defn execute-sql 
  "Execute a SQL query.
  
  Arguments
  db -- db
  sql -- string, a SQL query
  params -- [literals], a list of bind values 

  Returns
  A map with the following keys
   :rows -- a list of row tuples if any
   :update-count -- int, the number of updated rows if any
   :generated-keys -- list, a list of generated keys (insert / sequence IDs).  Usually like [[25]] 
  "
  ([db sql] (execute-sql db sql []))
  ([db sql params]
   ;(println sql params)
   (let [conn (get-connection db)]
     (with-open [stmt (.prepareStatement conn sql)]
       (dorun (map-indexed 
                (fn [idx p] (.setObject stmt (inc idx) p)) params))
       (let [has-result-set (.execute stmt)]
         {:sql sql 
          :params params
          :rows (if has-result-set 
                  (with-open [rs (.getResultSet stmt)] (resultset-seq2 rs))
                  [])
          :update-count (if has-result-set 0 (.getUpdateCount stmt))
          :generated-keys (if has-result-set {} 
                            (resultset-seq2 (.getGeneratedKeys stmt)))})))))

(defmacro tx 
  "Perform a transaction for a database
 
  When an exception occurs inside of this block, a rollback is executed and the exception is re-thrown.

  For example:
    (tx db
      (execute-sql \"delete from my_table where id = ?\" [5]))
   
  " 
  [db & body] 
  `((execute-sql ~db "BEGIN" [])
    (try
      ~@body
      (execute-sql ~db "COMMIT")
      (catch Exception the-exception#
        (execute-sql ~db "ROLLBACK")
        (throw the-exception#)))))

(defmacro tx-read-only 
  "Run a block of commands for a database in a read-only transaction wrapper" 
  [db & body] 
  `((execute-sql ~db "BEGIN" [])
    (try
      ~@body
      (execute-sql ~db "ROLLBACK")
      (catch Exception the-exception#
        (execute-sql ~db "ROLLBACK")
        (throw the-exception#)))))

(defn column 
  "Define a column
  
  Arguments
  name -- string, the key of the column
  options
    :initial -- use this value on insert if none is provided 
    :default -- use this value on update if none is provided
    :name -- the name of the column in the table

  Returns
  Column
  "
  [a-keyword & params]
  (conj {:name (columnify a-keyword) 
         :key a-keyword
         :type :column
         :primary? false} (apply hash-map params)))

(defmacro deftype?
  "Create a function which checks the :type of a map

  This is useful for storing casual types in maps. 

  For example \"(deftype? car)\" will define a function named \"car?\" which checks its argument
  for having the :type key set to :car 

  Arguments
  a-key -- symbol, the name of the keyword and defined function
  "
  [a-key]
  (let [kw (keyword (name a-key))] 
     `(defn-
       ~(symbol (str (name a-key) "?"))
       [m#]
       (and (map? m#) 
            (= (:type m#) ~kw)))))

(deftype? column)
(deftype? table)
(deftype? bind)
(deftype? relation)
(deftype? infix)
(deftype? prefix)
(deftype? unary)
(deftype? on)
(deftype? where)
(deftype? relation)
(deftype? select)
(deftype? insert)
(deftype? update)
(deftype? delete)
(deftype? has-one)
(deftype? has-many)

(defn- parse-columns
  [opts]
  (let [col-opts (filter #(or (keyword? %1) (column? %1)) opts)
        ; convert keywords to columns
        cols0 (for [c col-opts]
                (if (keyword? c) (column c) c))
        ; default the first column to primary
        cols1 (if (some :primary? cols0)
                cols0
                (cons (assoc (first cols0) :primary? true) (rest cols0)))]
    cols1))

(defn deftable
  "Define a table

  This has a couple of argument forms

  Here are some examples:
    (deftable :company
      :id
      :title)
     
    (deftable :company
      (column :id)
      (column :title)
      (has-many :employees :user :company_id)) 

    (deftable :company
      (column :id)
      (column :title)
      {:schema some-schema}) 

  The first column specified defaults to the primary-key column.

  "
  [key & opts]
  (let [columns (parse-columns opts)
        opts0 (->> (cons {:schema +schema+} opts)
                (filter #(and (map? %) (not (column? %))))
                (reduce conj)) 
        schema (:schema opts0)
        pk-column (first (filter :primary? columns))]

    (swap! (:tables schema) 
           assoc 
           key 
           {:name (name key)
            :type :table
            :columns columns
            :relations (filter relation? opts)})
    (doseq [fk (filter has-one? opts)]
      (add-foreign-key 
        schema 
        key fk)
      (when (:reverse fk)
        (add-foreign-key
          schema
          (:table fk)
          {:table key
           :column (:column fk)
           :type :has-many
           :key (:reverse fk)
           :reverse (:key fk)}))
      )
    (doseq [fk (filter has-many? opts)]
      (add-foreign-key 
        schema 
        key fk)
      (when (:reverse fk)
        (add-foreign-key
          schema
          (:table fk)
          {:table key
           :column (:column fk)
           :type :has-one
           :key (:reverse fk)
           :reverse (:key fk)}))
      )
    ))


(defn- lookup-table [schema entity-name] 
  (let [entity (get @(:tables schema) entity-name nil)]
    (if entity entity
      (throw (Exception. (format "unexpected entity \"%s\"" entity-name))))))

(defn- infix-op 
  ([op] nil)
  ([op ls] ls)
  ([op ls rs] {:type :infix :op op :left ls :right rs})
  ([op ls rs & more] (infix-op op 
                               (infix-op op ls rs)
                               (apply infix-op more))))

(defn infix 
  "Generate a function which takes two arguments
  
  For example:
    (infix \"+\") generates a function which takes two arguments and will
  compile to a SQL expression like \"$left + $right\"
  "
  [op]
  (fn [l r] {:type :infix :op op :left l :right r}))


(defonce infix-operators (atom '{
                                 = (atomic/infix "=")
                                 and (atomic/infix "AND")
                                 or (atomic/infix "OR")
                                 not (atomic/prefix "NOT")
                                 bit-and (atomic/infix "&")
                                 bit-or (atomic/infix "|")
                                 bit-not (atomic/prefix "~")
                                 bit-xor (atomic/infix "^")
                                 like (atomic/infix "LIKE")
                                 rlike (atomic/infix "RLIKE")
                                 is (atomic/infix "IS")
                                 + (atomic/infix "+")
                                 / (atomic/infix "/")
                                 * (atomic/infix "*")
                                 in (atomic/infix "IN")
                                 >= (atomic/infix ">=")
                                 != (atomic/infix "!=")
                                 <= (atomic/infix "<=")}))

(def select 
  "A empty select query
  
  Usage:
    (-> select
        (from :user)
        (where (= :name \"Brandon\"))
        (execute db))

  "
  {:type :select
   :relations []
   :where []
   :limit nil
   :group-by []})

(defn insert-into
  "Get an insert query
  
  Usage:
    (-> (insert-into :user {:name \"Brandon\"})
        (execute db))
  " 
  [table values]
  {:type :insert
   :table table
   :values values})

(defn update
  "Get an update query
  
  Usage:
    (-> (update :user {:name \"Brandon\"})
        (where (= :id 5))
        (execute db))
  "
  [table values]
  {:type :update
   :table table
   :values values
   :where []})

(defn delete [table] 
  "Get a delete query
  
  Usage:
    (-> (delete :user)
        (where (= :id 5))
        (execute db))
  "
  {:type :delete
   :table table
   :where []})

(def relation 
  {:type :relation
   :as nil
   :is-left-outer false
   :source-table nil
   :on nil
   :source-query nil})
 
(defn from 
  [query
   relation-name
   & exprs]
  "Create a from relation"
  (let [[on-exprs attr-exprs] (classify on? exprs)
        on-query (apply infix-op "AND" (flatten (map :items on-exprs)))
        new-relation (apply conj relation {:as relation-name :source-table relation-name :on on-query} attr-exprs)
        new-query (assoc query :relations (concat (:relations query) [new-relation]))]
    new-query))

(defn columns
  "Specify a list of (columns [:u.id [:user :id] ] "
  [query & cols]
  (assoc query 
         :columns 
         (doall (for [[lhs rhs] (partition 2 cols)]
                  {:expr lhs :key-path rhs}))))

(defn parse-dsl 
  [clauses]
  (clojure.walk/postwalk-replace @infix-operators clauses))

(defmacro on 
  "Add an ON clause to a join clause
  
  See \"where\"
  "
  [& clauses]
  `(hash-map :type :on
             :items (list ~@(parse-dsl clauses))))

(defn join [& expr] (apply from expr))
(defn inner-join [rel & exprs] (apply from rel exprs))
(defn left-join [rel & exprs] (apply from rel (cons {:is-left-outer true} exprs)))

(defmacro where 
  "Add a where clause to a query. 

  Arguments
  query -- a query
  clauses* -- one or more clauses

  Returns
  query
  "
  [query & clauses]
  `(assoc ~query :where (concat (:where ~query) (list ~@(parse-dsl clauses)))))

(defn- compile-opt-clause [schema query] [])

(defn- get-columns 
  [schema query]
  (if (not (= nil (:columns query)))
    (:columns query)
    (flatten 
      (for [relation (:relations query)]
        (if (= nil (:source-table relation))
          []
          (let [table (lookup-table schema (:source-table relation))]
            (for [column (:columns table)]
              (let [col-name (:name column)
                    rel-alias (name (:as relation))
                    key-path [(:as relation) (:key column)]
                    expr (keyword (format "%s.%s" rel-alias col-name))]
                {:key-path key-path :expr expr}))))))))

(defn sql
  "Generate a SQL expression"
  [text & values] 
  {:type :sql
   :text text
   :values values})

(defn bind 
  "Bind a value to be escaped"
  [v]
  {:type :bind :value v})

(defn- compile-keyword-expr 
  "Compile a keyword where expr"
  [query kw] 
  (name kw))

(defn- compile-literal-expr 
  [subject]
  [(bind subject)])

(defn- compile-sequential-expr
  [query expr]
  (concat 
    ["("]
    (interpose [","] (apply concat (map compile-literal-expr expr)))
    [")"]))

(defn- compile-expr 
  "compile a where expression"
  [query expr]
  (cond 
    (string? expr) (compile-literal-expr expr) 
    (integer? expr) (compile-literal-expr expr)
    (number? expr) (compile-literal-expr expr)
    (float? expr) (compile-literal-expr expr)
    (keyword? expr) (compile-keyword-expr query expr)
    (sequential? expr) (compile-sequential-expr query expr)
    (set? expr) (compile-sequential-expr query expr)
    (infix? expr) (concat 
                    ["("] 
                    (compile-expr query (:left expr)) 
                    [" " (:op expr) " "] 
                    (compile-expr query (:right expr))
                    [")"])
    (unary? expr) (concat 
                    ["(" (:op expr)] 
                    (compile-expr query (:expr expr))
                    [")"])
    :else (throw (Exception. (str "unexpected expression: " expr)))))

(defn- compile-order-clause [schema query] [])

(defn- compile-where-clause 
  [schema query] 
  (if (empty? (:where query))
    [] ; empty where
    (let [where-exprs0 (map #(compile-expr query %1) (:where query))
          where-exprs (apply concat (interpose [" AND "] where-exprs0))]
      (cons " WHERE " where-exprs))))

(defn- compile-col-clause [schema query] 
  (interpose ", "
    (for [column (get-columns schema query)]
      (compile-expr query (:expr column)))))

(defn- compile-limit-clause [schema query] [])

(defn- partition-bind 
  [exprs] 
  (let [bind-pair (fn [e] (if (bind? e) ["?" [(:value e)]] [e []]))
        exprs0 (map bind-pair exprs)
        sql (map first exprs0)
        binds (flatten (map second exprs0))]
    [sql binds]))

(defn- compile-on
  [schema relation]
  (if (= nil (:on relation))
    []
    [" ON " (compile-expr schema (:on relation))]))

(defn- compile-relation
  [schema relation]
  ; fix-me: compile sub-selects here.
  (let [relation-name (:name (lookup-table schema (:source-table relation)))
        relation-alias (name (:as relation))
        relation-part [relation-name " AS " relation-alias]]
    (concat relation-part (compile-on schema relation))))

(defn- compile-from-clause 
  "Get the 'FROM' part of a select query"
  [schema query]
  (if (:relations query)
    (let [first-relation (first (:relations query))
          other-relations (rest (:relations query))]
      (flatten 
        [[" FROM "]
        (compile-relation schema first-relation)
        (flatten 
          (for [relation other-relations]
            [" INNER JOIN " (compile-relation schema relation)]))]))
         []))

(defn- compile-select 
  "Compile a select query" 
  [schema query] 
  (let [exprs (flatten [["SELECT "] 
                        (compile-opt-clause schema query)
                        (compile-col-clause schema query)
                        (compile-from-clause schema query)
                        (compile-where-clause schema query)
                        (compile-order-clause schema query)
                        (compile-limit-clause schema query)])
        [sql bind] (partition-bind exprs)] 
    {:text (clojure.string/join sql) 
     :column-key-paths (map :key-path (get-columns schema query))
     :bind bind}))

(defn get-col-vals 
  [schema table kw]
  (let [t (get @(:tables schema) table)
        cols (filter #(contains? % kw) (:columns t))
        kvseq (for [col cols] [(:key col) (get col kw)])
        initials (apply hash-map (apply concat kvseq))]
    initials))

(defn- get-insert-defaults 
  [schema table]
  (get-col-vals schema table :initial))
               
(defn- get-update-defaults 
  [schema table]
  (get-col-vals schema table :default))

(defn- compile-insert-values
  [schema query]
  (let [values0 (:values query)
        initials (get-col-vals schema (:table query) :initial)
        values (default values0 initials)
        colvals (seq values)]
    (if (empty? values)
      [] 
      (concat 
        ["("]
        (interpose "," (for [[k v] colvals] (name k)))
        [") VALUES ("]
        (interpose "," (for [[k v] colvals] (bind v)))
        [")"]))))

(defn- compile-insert 
  "Compile an insert query"
  [schema query]
  (let [exprs (apply concat [["INSERT INTO " (name (:table query)) " "]
                             (compile-insert-values schema query)])
        [sql bind] (partition-bind exprs)]
    {:text (clojure.string/join sql)
     :bind bind
     :column-key-paths []}))

(defn- compile-update-values
  [schema query]
  (let [values0 (:values query)
        defaults (get-col-vals schema (:table query) :default)
        values (default values0 defaults)]
    (concat 
      [" SET "]
      (apply concat 
             (interpose [", "]
                        (for [[k v] values]
                          [(name k) " = " (bind v)]))))))

(defn- compile-update 
  "Compile an insert query"
  [schema query]
  (let [exprs (apply concat [["UPDATE " (name (:table query)) " "]
                             (compile-update-values schema query)
                             (compile-where-clause schema query)])
        [sql bind] (partition-bind exprs)]
    {:text (clojure.string/join sql)
     :bind bind
     :column-key-paths []}))

(defn- compile-delete 
  "Compile a delete query"
  [schema query]
  (let [exprs (apply concat 
                     [["DELETE FROM " (name (:table query)) " "]
                      (compile-where-clause schema query)])
        [sql bind] (partition-bind exprs)]
    {:text (clojure.string/join sql)
     :bind bind
     :column-key-paths []}))

(defn compile-query
  "Compile a query.

  Arguments
  query -- map, the query

  Returns
  A map with the following keys
    :text -- string, the SQL text
    :bind -- list, a list of literals to bind
  "
  [query schema]
  (cond 
    (select? query) (compile-select schema query)
    (insert? query) (compile-insert schema query)
    (update? query) (compile-update schema query)
    (delete? query) (compile-delete schema query)
    :else (throw (Exception. "unexpected query"))))

(defn execute
  "Run a query against an db"
  [query db]
  (let [compiled (compile-query query (:schema db))
        result (execute-sql db (:text compiled) (:bind compiled))
        rows (:rows result)
        key-paths (:column-key-paths compiled)
        ]
      (assoc result 
             :insert-id (if (insert? query) 
                          (first (first (:generated-keys result)))
                          nil)
             :rows (unflatten rows key-paths))))

(defn get-primary-key
  [schema table]
  (first (filter :primary? (:columns (get @(:tables schema) table)))))

(defn make-query
  "make a query given an entity and some where-conditions"
  [db entity where-parts]
  (let [table (lookup-table (:schema db) entity)
        key-paths (for [c (:columns table)] [(:key c)])
        cols (reduce concat (for [column (:columns table)]
                        [(keyword (format "%s.%s" (name entity) (:name column))) ; column expr
                         [(:key column)]
                         ])) ; key path
        ; for some reason -> doesn't work here:
        from-q (from select entity)
        cols-q (apply columns from-q cols)
        where-q (assoc cols-q :where where-parts)]
    where-q))

(defn- get-table 
  [schema src-table [h & more]]
  (if (not h)
    src-table
    (get-table schema (:table (get @(:foreign-keys schema) [src-table h])) more)))

(defn join-to 
  "Join a set of dotted relation-paths to a result set.
  
  In other words, fill-in a list of related row objects.

  For instance if you have a schema like:

    driver (id, name)
    car (id, driver_id, garage_id, name)
    garage (id, house_id)
    house (id)

  (join-to db :review [:user :user.emails] [{:id 1 :user_id 3}]) =>
    [{:id 1 :user_id 3 :user { :id 3 :emails [{:id 1 :address \"foo@bar.com\"}]}}]
  "
  [db table relation-paths result-set]
  (let [relation-paths0 (sort (parse-relation-paths relation-paths))
        schema (:schema db)
        result-set0 (atom result-set)]
    (doseq [relation-path relation-paths0]
      (let [parent-table-key (get-table schema table (all-but-last relation-path))
            relation-key (last relation-path)
            foreign-key (get @(:foreign-keys schema) [parent-table-key relation-key])
            remote-table (:table foreign-key)
            foreign-key-type (:type foreign-key)
            remote-primary-key-col (get-primary-key schema remote-table)
            remote-primary-key (:key remote-primary-key-col)
            remote-primary-key-name (:name remote-primary-key-col)
            column (:column foreign-key)]
        (when (not foreign-key)
          (throw (Exception. (format "expecting a foreign key for %s" relation-key))))
        (when (not remote-table)
          (throw (Exception. (format "expecting a remote table for %s" relation-key))))
        (cond 
          (= foreign-key-type :has-one)
          ; has-one case
          ; get all of the x.y.column values
          (let [col-vals (apply hash-set (map column (get-in2 (all-but-last relation-path) @result-set0)))
                ; Select the remote rows
                rows (map relation-key (:rows (-> select
                       (from remote-table {:as relation-key})
                       (where (in (join-keywords relation-key
                                                 remote-primary-key)
                                  col-vals))
                       (execute db))))
                ; Get (remote primary key) -> (remote row)
                pk-to-row (zipmap (map remote-primary-key rows) rows)
                ]
            ; Store the result
            (reset! result-set0 (map-in 
                                (fn [item] 
                                  (assoc item relation-key (get pk-to-row (get item column))))
                                (all-but-last relation-path)
                                @result-set0)))
          ; handle a has-many
          (= foreign-key-type :has-many) 
          (let [local-primary-key :id ; fixme
                items (get-in2 (all-but-last relation-path) @result-set0)
                local-keys (map local-primary-key items)
                rows (map relation-key (:rows (-> select 
                                                (from remote-table {:as relation-key})
                       (where (in (join-keywords relation-key column) local-keys))
                       (execute db))))
                local-primary-key-to-rows (get-key-to-seq rows column)
                get-val (fn [item] (get local-primary-key-to-rows (get item local-primary-key []) []))
                replace-item (fn [item] (assoc item relation-key (get-val item)))
                new-result-set (map-in replace-item (all-but-last relation-path) @result-set0)]
            (reset! result-set0 new-result-set)
          ) 
          :else (throw (Exception. (format "unexpected foreign key type: %s" foreign-key-type))))))
    @result-set0))

(defmacro one 
  "Get one item

  Examples

    : Load a user with id 5
    (one db :user (> :id 5)) 

    ; Load a user with id 3, and review and review.business joined
    (one db :user (= :id 3) :review :review.business) 
    
    ; Load a user with id 5 and email joined
    (one db :user (= :id 5) :email) 
    
    ; Load a user with id 5 and email joined
    (one db :user (= :id 5) :emails)) 
  "
  [db entity & options]
  `(let [where# (list ~@(parse-dsl (filter not-keyword? options)))
         join-key-paths# (list ~@(filter keyword? options))
         query# (make-query ~db ~entity where#)
         result# (execute query# ~db)
         joined-rows# (join-to ~db ~entity join-key-paths# (take 1 (:rows result#)))]
       (first joined-rows#)))

(defmacro many 
  "Get many items"
  [db entity & options]
  `(let [where# (list ~@(parse-dsl (filter not-keyword? options)))
         join-key-paths# (list ~@(filter keyword? options))
         query# (make-query ~db ~entity where#)
         result# (execute query# ~db)
         joined-rows# (join-to ~db ~entity join-key-paths# (:rows result#))]
       joined-rows#))

(defn create
  "Create a row, returning its insert ID"
  [db table props]
  (-> (insert-into table props)
    (execute db)
    (:insert-id)))
     
