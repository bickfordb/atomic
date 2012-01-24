{:namespaces
 ({:source-url nil,
   :wiki-url "atomic-api.html",
   :name "atomic",
   :doc nil}
  {:source-url nil,
   :wiki-url "atomic.localmap-api.html",
   :name "atomic.localmap",
   :doc nil}
  {:source-url nil,
   :wiki-url "atomic.util-api.html",
   :name "atomic.util",
   :doc nil}),
 :vars
 ({:arglists ([v]),
   :name "bind",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/bind",
   :doc "Bind a value to be escaped",
   :var-type "function",
   :line 358,
   :file "src/atomic.clj"}
  {:arglists ([a-keyword & params]),
   :name "column",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/column",
   :doc
   "Define a column\n\nArguments\nname -- string, the key of the column\noptions\n  :initial -- use this value on insert if none is provided \n  :default -- use this value on update if none is provided\n  :name -- the name of the column in the table\n\nReturns\nColumn\n",
   :var-type "function",
   :line 135,
   :file "src/atomic.clj"}
  {:arglists ([query & cols]),
   :name "columns",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/columns",
   :doc "(columns [:u.id [:user :id] ] ",
   :var-type "function",
   :line 315,
   :file "src/atomic.clj"}
  {:arglists ([schema query]),
   :name "compile-delete",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/compile-delete",
   :doc "Compile a delete query",
   :var-type "function",
   :line 535,
   :file "src/atomic.clj"}
  {:arglists ([query expr]),
   :name "compile-expr",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/compile-expr",
   :doc "compile a where expression",
   :var-type "function",
   :line 380,
   :file "src/atomic.clj"}
  {:arglists ([schema query]),
   :name "compile-from-clause",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/compile-from-clause",
   :doc "Get the 'FROM' part of a select query",
   :var-type "function",
   :line 442,
   :file "src/atomic.clj"}
  {:arglists ([schema query]),
   :name "compile-insert",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/compile-insert",
   :doc "Compile an insert query",
   :var-type "function",
   :line 502,
   :file "src/atomic.clj"}
  {:arglists ([query kw]),
   :name "compile-keyword-expr",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/compile-keyword-expr",
   :doc "Compile a keyword where expr",
   :var-type "function",
   :line 364,
   :file "src/atomic.clj"}
  {:arglists ([schema query]),
   :name "compile-select",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/compile-select",
   :doc "Compile a select query",
   :var-type "function",
   :line 456,
   :file "src/atomic.clj"}
  {:arglists ([schema query]),
   :name "compile-update",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/compile-update",
   :doc "Compile an insert query",
   :var-type "function",
   :line 524,
   :file "src/atomic.clj"}
  {:arglists ([db table props]),
   :name "create",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/create",
   :doc "Create a row, returning its insert ID",
   :var-type "function",
   :line 702,
   :file "src/atomic.clj"}
  {:arglists ([driver url & opts]),
   :name "create-db",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/create-db",
   :doc "Create a new db",
   :var-type "function",
   :line 16,
   :file "src/atomic.clj"}
  {:arglists ([]),
   :name "create-schema",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/create-schema",
   :doc "create a schema object",
   :var-type "function",
   :line 8,
   :file "src/atomic.clj"}
  {:arglists ([key & opts]),
   :name "deftable",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/deftable",
   :doc "Add a table to a schema",
   :var-type "function",
   :line 194,
   :file "src/atomic.clj"}
  {:arglists ([query db]),
   :name "execute",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/execute",
   :doc "Run a query against an db",
   :var-type "function",
   :line 555,
   :file "src/atomic.clj"}
  {:arglists ([db sql] [db sql params]),
   :name "execute-sql",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/execute-sql",
   :doc "Execute a query",
   :var-type "function",
   :line 103,
   :file "src/atomic.clj"}
  {:arglists ([key table column] [key table column reverse]),
   :name "has-many",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/has-many",
   :doc "Get a has-many foreign key",
   :var-type "function",
   :line 55,
   :file "src/atomic.clj"}
  {:arglists ([key table column] [key table column reverse]),
   :name "has-one",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/has-one",
   :doc "Get a has-one foreign key",
   :var-type "function",
   :line 45,
   :file "src/atomic.clj"}
  {:arglists ([op]),
   :name "infix",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/infix",
   :doc "Generate an infix expression",
   :var-type "function",
   :line 255,
   :file "src/atomic.clj"}
  {:arglists ([db table relation-paths result-set]),
   :name "join-to",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/join-to",
   :doc
   "Join a set of dotted relation-paths to a result set.\n\nIn other words, fill-in a list of related row objects.\n\nFor instance if you have a schema like:\n\n  driver (id, name)\n  car (id, driver_id, garage_id, name)\n  garage (id, house_id)\n  house (id)\n\n(join-to db :review [:user :user.emails] [{:id 1 :user_id 3}]) =>\n  [{:id 1 :user_id 3 :user { :id 3 :emails [{:id 1 :address \"foo@bar.com\"}]}}]\n",
   :var-type "function",
   :line 594,
   :file "src/atomic.clj"}
  {:arglists ([db entity where-parts]),
   :name "make-query",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/make-query",
   :doc "make a query given an entity and some where-conditions",
   :var-type "function",
   :line 573,
   :file "src/atomic.clj"}
  {:arglists ([db entity & options]),
   :name "many",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/many",
   :doc "Get many items",
   :var-type "macro",
   :line 692,
   :file "src/atomic.clj"}
  {:arglists ([db entity & options]),
   :name "one",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/one",
   :doc
   "Get one item\n\nExamples\n\n  : Load a user with id 5\n  (one db :user (> :id 5)) \n\n  ; Load a user with id 3, and review and review.business joined\n  (one db :user (= :id 3) :review :review.business) \n  \n  ; Load a user with id 5 and email joined\n  (one db :user (= :id 5) :email) \n  \n  ; Load a user with id 5 and email joined\n  (one db :user (= :id 5) :emails)) \n",
   :var-type "macro",
   :line 666,
   :file "src/atomic.clj"}
  {:file "src/atomic.clj",
   :raw-source-url nil,
   :source-url nil,
   :wiki-url "/atomic-api.html#atomic/select",
   :namespace "atomic",
   :line 271,
   :var-type "var",
   :doc "get a select query",
   :name "select"}
  {:arglists ([db & body]),
   :name "tx",
   :namespace "atomic",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic/tx",
   :doc "Perform a transaction",
   :var-type "macro",
   :line 122,
   :file "src/atomic.clj"}
  {:arglists ([]),
   :name "create",
   :namespace "atomic.localmap",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.localmap/create",
   :doc "Create a thread local-map",
   :var-type "function",
   :line 4,
   :file "src/atomic/localmap.clj"}
  {:arglists ([localmap key]),
   :name "get",
   :namespace "atomic.localmap",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.localmap/get",
   :doc "Lookup a key",
   :var-type "function",
   :line 16,
   :file "src/atomic/localmap.clj"}
  {:arglists ([localmap key val]),
   :name "set",
   :namespace "atomic.localmap",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.localmap/set",
   :doc "Set a value for key",
   :var-type "function",
   :line 26,
   :file "src/atomic/localmap.clj"}
  {:arglists ([localmap key func]),
   :name "setdefault",
   :namespace "atomic.localmap",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.localmap/setdefault",
   :doc "Set a default value for key",
   :var-type "macro",
   :line 6,
   :file "src/atomic/localmap.clj"}
  {:arglists ([localmap]),
   :name "to-map",
   :namespace "atomic.localmap",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.localmap/to-map",
   :doc "Get a map",
   :var-type "function",
   :line 21,
   :file "src/atomic/localmap.clj"}
  {:arglists ([a-seq]),
   :name "all-but-last",
   :namespace "atomic.util",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.util/all-but-last",
   :doc "Get all but the last element of a sequence",
   :var-type "function",
   :line 82,
   :file "src/atomic/util.clj"}
  {:arglists ([func key-paths subj]),
   :name "each-in",
   :namespace "atomic.util",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.util/each-in",
   :doc "Call func for each value in subj matching key-path",
   :var-type "function",
   :line 54,
   :file "src/atomic/util.clj"}
  {:arglists ([a]),
   :name "heads",
   :namespace "atomic.util",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.util/heads",
   :doc "Get all of the prefixes ef a",
   :var-type "function",
   :line 37,
   :file "src/atomic/util.clj"}
  {:arglists ([func key-paths subj]),
   :name "map-in",
   :namespace "atomic.util",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.util/map-in",
   :doc "Replace each value in subj with matching key-path",
   :var-type "function",
   :line 69,
   :file "src/atomic/util.clj"}
  {:arglists ([rows key-paths]),
   :name "unflatten",
   :namespace "atomic.util",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.util/unflatten",
   :doc "Build a nested result set from a set of key paths",
   :var-type "function",
   :line 112,
   :file "src/atomic/util.clj"}
  {:arglists ([& seqs]),
   :name "zipn",
   :namespace "atomic.util",
   :source-url nil,
   :raw-source-url nil,
   :wiki-url "/atomic-api.html#atomic.util/zipn",
   :doc "Python / ML / Haskell style zip",
   :var-type "function",
   :line 17,
   :file "src/atomic/util.clj"})}