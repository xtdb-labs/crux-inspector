(ns crux.fixtures.query-plan
  (:require [crux.inspector.instrument :as instr]
            [crux.inspector.trace :as t]
            [crux.index :as i]
            [loom.graph :as g]
            [loom.io :as lio]
            [rhizome.viz :as v]))

(defprotocol Children
  (children [i]))

(extend-protocol Children
  crux.index.NAryConstrainingLayeredVirtualIndex
  (children [this]
    [(:n-ary-index this)])

  crux.index.NAryJoinLayeredVirtualIndex
  (children [this]
    (:unary-join-indexes this))

  crux.index.UnaryJoinVirtualIndex
  (children [this]
    (:indexes this))

  crux.index.BinaryJoinLayeredVirtualIndex
  (children [^crux.index.BinaryJoinLayeredVirtualIndex this]
    (let [state ^crux.index.BinaryJoinLayeredVirtualIndexState (.state this)]
      (.indexes state)))

  crux.index.RelationVirtualIndex
  (children [^crux.index.RelationVirtualIndex this]
    (let [state ^crux.index.RelationIteratorsState (.state this)]
      (.indexes state)))

  Object
  (children [this]
    nil))

(comment
  (lio/view (-> (g/graph)
                (g/add-nodes 1 2 3)
                (g/add-edges [1 2] [1 3 10])))

  (def g
    {:a [:b :c]
     :b [:c]
     :c [:a]})

  (v/view-graph (keys g) g
                :node->descriptor (fn [n] {:label n}))

  #_(v/view-tree sequential? seq g
               :node->descriptor (fn [n] {:label (when (number? n) n)})))

(defn loom-instrumenter [g visited i]
  (swap! g assoc (t/index-name i) (mapv t/index-name (children i)))
  i)

(defn instrument-layered-idx->seq [idx]
  (let [g (atom {})
        f (partial loom-instrumenter g)]
    (let [x (instr/instrumented-layered-idx->seq f idx)]
      (clojure.pprint/pprint @g)
      (v/view-graph (keys @g) @g
                    :node->descriptor (fn [n] {:label n}))
      x)))

(defmacro with-plan [& form]
  `(with-redefs [i/layered-idx->seq instrument-layered-idx->seq]
     ~@form))
