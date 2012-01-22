
# &#9883; ATOMIC &#9883;

A small SQL library for Clojure

## Installation

 *TBD this doesn't work yet* 

 Add ["atomic" 0.1] to your project.clj file

## Usage

```clj
(use 'atomic)
(def schema (create-schema))

; Describe a user table 
(add-table 
  schema 
  :user
  :id
  :name
  (has-many :emails :email :user_id :user))

(add-table 
  schema 
  :email
  :id
  :address
  :user_id)

(def engine (create-engine schema "org.sqlite.JDBC" "jdbc:sqlite::memory:"))
(execute-sql db "create table user (id integer primary key, name text, created_at integer)")
(execute-sql db "create table email (id integer primary key, user_id integer, address text)")

(insert engine :user {:id 1 :name "Brandon"})
(insert engine :email {:address "foo@bar.com" :user_id 1})
(insert engine :email {:address "bar@bar.com" :user_id 1})

(println (-> select 
             (from :user)
             (join :email (on (= :email.user_id :user.id)))
             (where (= :id 1))
             (execute engine)))
; [{:user {:name "Brandon" :id 5} :email {:address "foo@bar.com" :user_id 1 :id 1}}
;  {:user {:name "Brandon" :id 5} :email {:address "bar@bar.com" :user_id 1 :id 2}}]

; Easy-join graph API (one/many):
; Get the "Brandon" record, and join in the related emails

(println (one db :user :emails (= (:name "Brandon")))) 
; [{:name "Brandon" :id 5 :emails [{:address "foo@bar.com" :user_id 1 :id 1} {:address "bar@bar.com" :user_id 1 :id 2}]}]
```






