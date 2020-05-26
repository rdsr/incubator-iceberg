(ns iceberg-fu.orc
  (:require [clojure.data.json :as j]
            [iceberg-fu.schema :as s])
  (:import [org.apache.avro.generic GenericDatumReader]
           [java.io File]
           [java.util.function Function] 
           [org.apache.iceberg Files]
           [org.apache.iceberg.orc ORC OrcMetrics]
           [org.apache.iceberg.data.orc GenericOrcWriter]           
           [org.apache.iceberg.data RandomGenericData]))

(comment
  ;; write data
  (def f "/tmp/iceberg/data.orc4")
  (let [sc (s/schema
            (s/row
             (s/field :a 1 (s/primitive :string))))
        data (RandomGenericData/generate sc 10000000 0)
        orc-writer (->
                    (Files/localOutput (File. f))
                    ORC/write
                    (.schema sc)
                    (.createWriterFunc
                     (reify Function
                       (apply [_ td]
                         (GenericOrcWriter/buildWriter td))))
                    .build)]
    (with-open [w orc-writer]
      (doseq [r data]
        (.add w r))))

  ;; fetch metrics
  (let [m (OrcMetrics/fromInputFile (Files/localInput f))]
    m)
  )





org.apache.hadoop.hdfs.
