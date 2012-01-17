(ns atomic.graph)

(defmacro one 
  "Get one item

  Examples

    : Load a user with id 5
    (one :user (> :id 5)) 

    ; Load a user with id 3, and review and review.business joined
    (one :user (= :id 3) :review :review.business) 
    
    ; Load a user with id 5 and email joined
    (one :user (= :id 5) :email) 
    
    ; Load a user with id 5 and email joined
    (one :user (= :id 5) [:emails])) 
  "
  [entity selector]
  nil)

(defmacro many 
  "Get many items"
  [entity options]
  [])

