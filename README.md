# &#9883; ATOMIC &#9883;
##

A SQL library for Clojure

## Usage

```clj

(def schema
  (table :user
     (column :id)
     (column :first_name)
     (column :last_name)
     (column :password)
     (column :salt)))

(use 'atomic)
(with-pool (create-pool "jdbc:sqlite::memory:")
  (tx
    (exec-sql "CREATE TABLE user (id INTEGER primary key,
                                  first_name TEXT,
                                  last_name TEXT,
                                  password TEXT)")
    (INSERT schema :user {:first_name "Brandon"
                          :last_name "Bickford"
                          :password "open sesame"})
    (SELECT schema :user (= :last_name "Bickford"))))
```

## License

Copyright 2012 Brandon Bickford.  Please refer to LICENSE
