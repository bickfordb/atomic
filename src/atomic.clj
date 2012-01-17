(ns atomic
  (:refer-clojure :exclude [compile])
  (:require [atomic.localmap :as localmap]
            clojure.string
            clojure.walk))

(defn create-engine 
  "Create a new engine"
  [params] 
  {
   :params params 
   :local (localmap/create)})

(defonce jdbc-inited (atom false))
(defonce jdbc-libs (atom ["org.sqlite.JDBC" "org.postgresql.Driver"]))

(defn- safe-load-class [p]
  (try (Class/forName p) (catch ClassNotFoundException e)))

(defn init-jdbc []
  (if (not @jdbc-inited) 
    (doall (map safe-load-class @jdbc-libs))
    (swap! jdbc-inited true)))

(defonce rollback-class (get-proxy-class java.lang.Exception))

(defn rollback [] (throw rollback-class))

(defn create-connection
  "Create a connection for an engine"
  [engine]
  (init-jdbc)
  (let [url (:url (:params engine))]
    (let [c (java.sql.DriverManager/getConnection ^String url)]
      c)))

(defn get-connection
  "Get a connection from an engine"
  [engine]
  (let [m (:local engine)]
    (localmap/setdefault m :connection (fn [] (create-connection engine)))))


(defn get-result-set2
  [statement]
  (with-open [result-set (.getResultSet statement)]
    (let [metadata (.getMetaData result-set)]
      (let [col-indices (range (.getColumnCount metadata))]
        (doall 
          (for [row-idx (iterate inc 0) 
                :while (and (.next result-set) (not (.isAfterLast result-set)))]
            (for [col-idx col-indices]
              ((fn [] 
                 (println col-idx)
                 (.getObject result-set ^int (int (inc col-idx))))))))
        ))))

(defn resultset-seq2
  "Creates and returns a vector of tuples corresponding to
  the rows in the java.sql.ResultSet rs"
  {:added "1.0"}
  [^java.sql.ResultSet rs]
    (let [idxs (range 1 (inc (.getColumnCount (.getMetaData rs))))
          row-values (fn [] (doall (map #(.getObject rs ^Integer %1) idxs)))
          result (transient (vector))]
      (loop []
        (when (.next rs)
          (conj! result (row-values))
          (recur)))
      (persistent! result))) 

(defn get-result-set
  [statement]
  (with-open [result-set (.getResultSet statement)]
    (resultset-seq2 result-set)))

(defn execute 
  "Execute a query"
  ([engine sql] (execute engine sql []))
  ([engine sql params]
  (let [conn (get-connection engine)]
    (with-open [stmt (.prepareStatement conn sql)]
      (dorun (map-indexed (fn [idx p] (.setObject stmt (inc idx) p)) params))
      (let [has-result-set (.execute stmt)]
        {:sql sql 
         :params params
         :rows (if has-result-set (get-result-set stmt) [])
         :update-count (if has-result-set 0 (.getUpdateCount stmt))})))))

(defmacro tx 
  "Perform a transaction" 
  [engine & body ] 
  `(       
    (exec ~engine "START TRANSACTION")
    ~@body
    (exec ~engine "COMMIT")
    (exec ~engine "ROLLBACK")
    ))

(defn columnify
  [kw]
  (clojure.string/replace (name kw) "-" "_"))

(defn column 
  "Define a column"
  [x & params]
  (conj {:name (columnify x) :key x} (apply hash-map params)))

(defn column? [t] (= (:type t) :column))
(defn table? [t] (= (:type t) :table))
(defn relation? [t] (= (:type t) :relation))
(defn infix? [t] (= (:type t) :infix))
(defn prefix? [t] (= (:type t) :prefix))
(defn unary? [t] (= (:type t) :unary))
(defn on? [t] (= (:type t) :on))

(defn create-schema
  "create a schema object"
  []
  {:tables (atom {})})

(defn deftable
  "Add an entity"
  [schema key & opts]
  (swap! (:tables schema) 
         assoc 
         key 
         {:name (name key)
          :type :table
          :columns (concat (filter column? opts)
                           (map column (filter keyword? opts)))
          :relations (filter relation? opts)}))


(defn lookup-table [schema entity-name] 
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
  "Generate an infix expression"
  [op]
  (fn [l r] {:type :infix :op op :left l :right r}))

(defn sql-and
  [& xs]
  (apply infix-op "AND" xs))

(defn sql-func
  [func]
  (fn [& args] {:type :func}))

(defonce infix-operators (atom '{
                                 = (infix "=")
                                 + (infix "+")
                                 / (infix "/")
                                 * (infix "*")
                                 qq (infix "qq")
                                 >= (infix ">=")
                                 != (infix "!=")
                                 <= (infix "<=")}))

; hello dictionary types
(defn where? [x] (and (map? x) (= (:type x) :where)))
(defn relation? [x] (and (map? x) (= (:type x) :relation)))
(defn select? [x] (and (map? x) (= (:type x) :select)))
(defn insert? [x] (and (map? x) (= (:type x) :insert)))
(defn update? [x] (and (map? x) (= (:type x) :update)))

(def select 
  "get a select query"
  {:type :select
   :relations []
   :where []
   :limit nil
   :group-by []})

(def relation 
  {:type :relation
   :as nil
   :is-left-outer false
   :source-table nil
   :on nil
   :source-query nil})
 
(defn parts 
    [f coll]
    [(filter f coll) (filter #(not (f %1)) coll)])
 
(defn from 
  [query
   relation-name
   & exprs]
  (let [[on-exprs attr-exprs] (parts on? exprs)
        on-query (apply infix-op "AND" (flatten (map :items on-exprs)))
        new-relation (apply conj relation {:as relation-name :source-table relation-name :on on-query} attr-exprs)
        new-query (assoc query :relations (concat (:relations query) [new-relation]))]
    new-query))

(defn parse-dsl 
  [clauses]
  (clojure.walk/postwalk-replace @infix-operators clauses))

(defmacro on 
  [& clauses]
  `(hash-map :type :on
             :items (list ~@(parse-dsl clauses))))

(defn join [& expr] (apply from expr))
(defn inner-join [rel & exprs] (apply from rel exprs))
(defn left-join [rel & exprs] (apply from rel (cons {:is-left-outer true} exprs)))

(defmacro where 
  [query & clauses]
  `(assoc ~query :where (concat (:where ~query) (list ~@(parse-dsl clauses)))))

(defn compile-opt-clause [schema query] [])

(defn get-columns 
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

(defn bind 
  "Bind a value to be escaped"
  [v]
  {:type :bind :value v})

(defn bind? [p] (and (map? p) (= (:type p) :bind)))
(defn compile-keyword-expr 
  "Compile a keyword where expr"
  [query kw] 
  (name kw))

(defn compile-literal-expr 
  [subject]
  [(bind subject)])

(defn compile-expr 
  "compile a where expression"
  [query expr]
  (cond 
    (string? expr) (compile-literal-expr expr) 
    (integer? expr) (compile-literal-expr expr)
    (number? expr) (compile-literal-expr expr)
    (keyword? expr) (compile-keyword-expr query expr)
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

(defn compile-order-clause [schema query] [])

(defn compile-where-clause 
  [schema query] 
  (if (empty? (:where query))
    [] ; empty where
    (let [where-exprs0 (map #(compile-expr query %1) (:where query))
          where-exprs (interpose " AND " where-exprs0)]
      (cons " WHERE " where-exprs))))

(defn compile-col-clause [schema query] 
  (interpose ", "
    (for [column (get-columns schema query)]
      (compile-expr query (:expr column)))))

(defn compile-limit-clause [schema query] [])

(defn partition-bind 
  [exprs] 
  (let [bind-pair (fn [e] (if (bind? e) ["?" [(:value e)]] [e []]))
        exprs0 (map bind-pair exprs)
        sql (map first exprs0)
        binds (flatten (map second exprs0))]
    [sql binds]))

(defn compile-on
  [schema relation]
  (if (= nil (:on relation))
    []
    [" ON " (compile-expr schema (:on relation))]))

(defn compile-relation
  [schema relation]
  ; fix-me: compile sub-selects here.
  (println "compile relation" relation)  
  (let [relation-name (:name (lookup-table schema (:source-table relation)))
        relation-alias (name (:as relation))
        relation-part [relation-name " AS " relation-alias]]
    (concat relation-part (compile-on schema relation))))

(defn compile-from-clause 
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

(defn compile-select 
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
     :column-paths []
     :bind bind}))

(defn build-result
  [rows key-paths]
  (let [num-key-paths (count key-paths)]
    (for [row rows]
      (loop [i 0
             result {}]
          (if (< i num-key-paths)
             (recur 
               (inc i)
               (assoc-in result (get key-paths i) (get row i)))
            result)))))

(defn compile-query
  [query schema]
  (cond 
    (select? query) (compile-select schema query)
    :else (throw (Exception. "unexpected query"))))

