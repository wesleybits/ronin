;; ##Ronin
;; ###Or: Ignoble Mercinaries With Scary Military Training
;; ####Or: Eastern Knights Errant
;; #####Or: Sane-er Access To A Kyoto Cabinet

;; Kyoto Cabinet is a pretty boss thing.  It's fast, light-weight,
;; and concurrency friendly.  It's primary data representation is
;; straight-up binary, so we can do some pretty wild things, like
;; key music files by arbetrary images.

;; However, it can be pretty insane to access via it's Java 
;; bindings.  Check out 
;; [it's DB docs](http://fallabs.com/kyotocabinet/javadoc/kyotocabinet/DB.html),
;; and scroll on over to `.open(String, int)`.  The `.open` 
;; specification alone is a short novella that gets into extreme 
;; detail over how to specify a fully tuned connection.  Not that 
;; you would need to get into such extreme detail when doing many 
;; light-weight things, and it's nice that it's there, but paths 
;; like `"-#log=/opt/kc/errors.log#logkind=error"`
;; can be daunting for new-commers.

;; But this is Clojure, so we can do _much_ better!

(ns ronin.core
  (:use ronin.specs)
  (:require [clojure.edn :as edn])
  (:import [kyotocabinet DB Visitor FileProcessor]
           [ronin.helper ByteArray]
           [java.util List ArrayList Map]))

(defn- byte-array?
  "Tests for an array of bytes."
  [elt]
  (= (type elt)
     (type (.getBytes "ronin"))))

(defn make-db
  "#Create a new DB object.

It takes only one optional parameter, `throws?`, which defaults to true.
When false, Kyoto Cabinet will not throw errors, so you'll have to check
the `DB.error()` method to see what went wrong."
  ([] (make-db true))
  ([throws?]
     (if throws? 
       (DB. DB/GEXCEPTIONAL)
       (DB.))))

(defn open-db!
  "#Open a DB object.

Takes a DB object and a map with the following elements:

 * `:type`: Required, must be one of the keys in `ronin.specs.db-specs`.
 * `:filename`: Sometimes required.  If you're using an in-memory DB,
   then this parameter is ignored.
 * `:tuning`: Optional map of string parameters and their values, as
   specified in [Kyoto Cabinet's DB docs](http://fallabs.com/kyotocabinet/javadoc/kyotocabinet/DB.html).  
   This option isn't cooked, much unlike all the other ones.

.. and a vector of access modes, which should be keys in `ronin.specs.open-options`.

Returns the opened DB object.

An example would be opening a DB on a file hash, in create and write mode, and performs
error logging to a file and compresses the resulting hash file when done:

    (open-db! my-db
      {:type :file-hash
       :filename \"./yojimbo\"
       :tuning {\"log\"      \"./yojimbo.log\",
                \"logkinds\" \"error\",
                \"opts\"     \"lc\"}}
      [:create :write])

When finished, be sure to `.close` it."
  [^DB db specs modes]
  (let [needs-filename #{:file-hash
                         :file-tree
                         :directory-hash
                         :directory-tree
                         :plain-text}

        db-type (do ;(println "checking DB type")
                    (assert (not (nil? (get db-specs (:type specs))))
                            ":type needs to be found in db-specs")
                    (:type specs))

        db-file (do ;(println "checking db file")
                    (if (some needs-filename [(:type specs)])
                      (do (assert (string? (:filename specs))
                                  ":filename needs to be a string")
                          (assert (not= "" (:filename specs))
                                  ":filename cannot be an empty string")
                          (str (:filename specs) (get db-specs db-type)))
                      (get db-specs db-type)))

        tuned-db-file (do; (println "checking tuning")
                          (if (and (map? (:tuning specs))
                                   (not (empty? (:tuning specs))))
                            (->> (:tuning specs)
                                 (map (fn [[k v]] (str k "=" v)))
                                 (reduce #(str %1 "#" %2))
                                 (str db-file "#"))
                            db-file))
        
        db-modes (do ;(println "checking modes")
                     (assert (vector? modes)
                             "modes must be a vector")
                     (assert (->> (map #(get open-options %) modes)
                                  (map number?)
                                  (reduce #(and %1 %2)))
                             "every specified mode must be in `open-options`")
                     (->> (map #(get open-options %) modes)
                          (apply bit-or 0)))]

    ;(println "done checking stuff")
    (.open db tuned-db-file db-modes)
    db))

(defmacro with-db
  "#Very Modestly Managed DB access.

This is a convienence macro that simply creates a DB object, opens it
with the provided spec map, executes the provided body and closes it 
afterward.  It does nothing else.  The opened DB object is bound to
the given symbol, so you can use it like this:

    (with-db [my-db {:type :cache-hash 
                     :modes [:create :read :write]
                     :throws? true}]
      (add-edn! my-db \"entry-1\" {:some \"data\"})
      (get-edn my-db \"entry-1\"))

Here, we do extend the open specification to include:
 
 * `:throws?`: should we throw errors or not?  Defaults to true if 
   not found.
 * `:modes`: should contain the desired modes you want to open the DB
   in.  This is required.

Returns the result of the last operation in `body`."
  [[db open-spec] & body]
  `(let [~db (make-db (get ~open-spec :throws? true))]
     (open-db! ~db ~open-spec (:modes ~open-spec))
     (let [res (do ~@body)]
       (.close ~db)
       res)))

(defmacro with-dbs
  "#Very Modestly Managed access to multiple DBs.

Much like `with-db`, but takes multiple DB and open-spec maps for it's
initial parameters, with simlar syntax to `let`.

An example would be merging hashes of people and animals together to see
who owns which pet, assuming that Ronin is being used to drive a graph 
database:

    (with-dbs [merged {:type :cache-hash 
                       :modes [:create :write :read]}
               people (assoc people-spec :modes [:read])
               animals (assoc animal-spec :modes [:read])]
      (merge-dbs! :add merged people animals)
      ;; ... perform your walk here ...
      )"
  [dbs-and-specs & body]
  (assert (not (empty? dbs-and-specs))
          "dbs-and-specs must not be empty.")
  (assert (even? (count dbs-and-specs))
          "dbs-and-specs must be of even length")
  (if (= 2 (count dbs-and-specs))
    `(with-db ~(vec dbs-and-specs) ~@body)
    `(with-db ~(vec (take 2 dbs-and-specs))
       (with-dbs ~(drop 2 dbs-and-specs)
         ~@body))))

;; --------
;; The following are specially abstracted forms that make using Kyoto Cabinet
;; with Clojure's EDN-like data easier.  Alternatives are available if rote binary
;; needs to be stored instead.

(defn- edn-str
  "Takes some Clojure and writes some EDN, safely."
  [elt]
  (let [cand (pr-str elt)]
    (assert (= elt (edn/read-string cand))
            "must be expressable as EDN")
    cand))

(defn add-bin! 
  "Adds a binary key and value to an open DB.  If the key exists, then
there is no change to the DB.

Returns `true` if it succeeds, `false` on an error."
  [^DB db ^bytes key ^bytes value]
  (.add db key value))

(defn add-edn!
  "Adds a string key and a Clojure data structure to an open DB.  It
uses `add-bin!` to do the heavy-lifting (which there isn't any).  If 
the key already exists in the database, then there is no change.

Returns `true` on success, `false` on an error."
  [^DB db ^String key value]
  (add-bin! db (.getBytes key)
            (bytes (.getBytes (edn-str value)))))

(defn get-bin
  "Gets binary data from an open DB using a binary key.

Returns a byte array on success, 'nil' if it errors."
  [^DB db ^bytes key]
  (.get db key))

(defn get-edn
  "Gets parsed Clojure data from an open DB using a string key.

Returns Clojure on success, 'nil' if it errors."
  [^DB db ^String key]
  (if-let [^bytes result (get-bin db (.getBytes key))]
    (-> (String. result)
        edn/read-string)))
