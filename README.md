# &#9883; ATOMIC &#9883;
## Description

Atomic is a minimalist SQL library for Clojure intended to make a joy out of performing queries against a SQL database.  It supports transactions, multiple connections and pools.  Instead of providing an object interface it provides a straightforward SQL DSL which executes queries and returns result sets as sequences of maps.  Also included are some utility functions to create Ruby-On-Rails style migrations.

## Usage

Atomic doesn't have much documentation at this point.  Here is a brief usage example:

```clj
(use 'atomic)

(def schema
  (table :user
     (column :id)
     (column :first_name)
     (column :last_name)
     (column :password)
     (column :salt)))

(with-pool (create-pool "jdbc:sqlite::memory:")
  (tx
    ; execute strings
    (exec-sql "CREATE TABLE user (id INTEGER PRIMARY KEY,
                                  first_name TEXT,
                                  last_name TEXT,
                                  password TEXT)")
    ; inserts!
    (?insert schema :user {:first_name "Brandon"
                          :last_name "Bickford"
                          :password "open sesame"})
    ; (NB passwords in this system use a two way hash)

    ; selects!
    (?select schema :user (?where (?= :last_name "Bickford")))
    ; => [{:first_name "Brandon" :last_name "Bickford" :id 1 :password "open sesame"}]

    ; joins!
    (?select schema :user (?join :user :u2 (?on (?= :first_name :u2.last_name))))))
    ; => [{:first_name "Brandon" :last_name "Bickford" :id 1 :password "open sesame" :u2.first_name "Brandon" :u2.last_name "Bickford" :u2.id 1 :u2.password "open sesame"}]
```

## License

Copyright 2012 Brandon Bickford.  Please refer to LICENSE
