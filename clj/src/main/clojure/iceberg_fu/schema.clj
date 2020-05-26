(ns iceberg-fu.schema
  (:import (org.apache.iceberg Schema)
           (org.apache.iceberg.types
            Type Types$NestedField
            Types$StructType Types$ListType Types$MapType
            Types$LongType Types$IntegerType Types$StringType Types$DateType Types$BooleanType Types$FloatType)))

(def iceberg-primitive-type
  {:long    (Types$LongType/get)
   :float   (Types$FloatType/get)
   :int     (Types$IntegerType/get)
   :boolean (Types$BooleanType/get)
   :string  (Types$StringType/get)
   :date    (Types$DateType/get)})

(defn field [fname id ftype & [opt]]
  (let [fname (name fname)]
    (if (= :opt opt)
      (Types$NestedField/optional id fname ftype)
      (Types$NestedField/required id fname ftype))))

(defn row [& fs]
  (Types$StructType/of fs))

(defn kv [kid ktype vid vtype & [opt]]
  (if (= :opt opt)
    (Types$MapType/ofOptional kid vid ktype vtype)
    (Types$MapType/ofRequired kid vid ktype vtype)))

(defn array [id etype & [opt]]
  (if (= :opt opt)
    (Types$ListType/ofOptional id etype)
    (Types$ListType/ofRequired id etype)))
  
(defn primitive [ptype]
  (if (contains? iceberg-primitive-type ptype)
    (iceberg-primitive-type ptype)
    (throw (ex-info
            (str "No Iceberg type found corresponding to primitive type " ptype)
            {}))))

(defn schema [rw-type]
  (Schema. (.fields rw-type)))

(comment
  (-> (schema
        (row (field :x 1 (primitive :int))
             (field :y 2 (primitive :int))
             (field :z 3 (kv 4 (primitive :int)
                             5 (row (field :a 6 (primitive :string))
                                   (field :b 7 (primitive :string)))))
             (field :p 8 (array 9 (kv 10 (row (field :x 12 (primitive :int) :opt))
                                      11 (row (field :y 13 (primitive :int) :opt)))))))
      println)
  )
