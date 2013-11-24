(ns ronin.core
  (:require [clojure.edn :as edn])
  (:import [kyotocabinet DB Visitor FileProcessor]
           [java.util List ArrayList Map]))

(defn- byte? 
  "Tests for a proper byte."
  [elt]
  (try 
    (= elt (byte elt))
    (catch Exception e 
      false)))

(defn- byte-array?
  "Tests for an array of bytes."
  [elt]
  (and (= (type elt)
          (type (to-array [])))
       (reduce #(and %1 (byte? %2))
               (vec elt))))

(def ^{:doc "A lookup table for Kyoto Cabinet's DB:open options.

 * `:synch`: auto synchronization, saves changes to the DB file.
 * `:transaction`: auto transaction, rewinds changes on error.
 * `:create`: create a casket (DB file), if it doesn't exist.
 * `:no-lock`: non-locking reads and writes, use with extreme care!
 * `:no-repair`: don't fix the DB if something goes wrong, use with care!
 * `:read`: read mode
 * `:write`: write mode
 * `:try-lock`: locking reads and writes. Safe but slow.
 * `:truncate`: open with data truncation. Improves speed, but screws with your data."}
  open-options
  {:sync        DB/OAUTOSYNC
   :transaction DB/OAUTOTRANS
   :create      DB/OCREATE
   :no-lock     DB/ONOLOCK
   :no-repair   DB/ONOREPAIR
   :read        DB/OREADER
   :truncate    DB/OTRUNCATE
   :try-lock    DB/OTRYLOCK
   :write       DB/OWRITER})

(def ^{:doc "A lookup table for merge options.

 * `:add`: add mode. Keep the existing value.
 * `:append`: append mode. Append the new value indiscriminately.
 * `:replace`: replace mode. Modify the old value.
 * `:overwrite`: overwrite mode. Replace the old value indiscriminately."}
  merge-options
  {:add       DB/MADD
   :append    DB/MAPPEND
   :replace   DB/MREPLACE
   :overwrite DB/MSET})

(def ^{:dynamic true
       :doc "##The Database Object.  

Dealt with using the `with-db` special form."}
  db nil)

(defn accept-bulk
  "## Accept Bulk

Sends a visitor to an array of keys.

 * `keys`: a vector of keys to visit.  Works best if they're strings.
 * `visitor`: the visitor used for this operation.
 * `write?`: optional arg, default is false.  Set to true if the visitor will be
   writing to the database.

Returns a boolean: true if successful, false if it failed

Note: Operations are performed atomically, so other threads trying to access the
listed records will be blocked.  To avoid deadlock, any explicit operations must
not be performed here."
  ([keys visitor] 
     (accept-bulk keys visitor false))
  ([keys visitor write?]
     (.accept_bulk db keys write?)))

(defn accept
  "#Accept

Sends a visitor to a single key.

 * `key`: a key to visit.  Works best if it's a string.
 * `visitor`: the visitor. 'Nuf said.
 * `write?`: optional arg, default is false.  Set to true if the visitor will be
writing to this key.

Returns a boolean: true if successful, false if it failed

Note: Operations are performed atomically, so other threads trying to access the
listed records will be blocked.  To avoid deadlock, any explicit operations must
not be performed here."
  ([key ^Visitor visitor] 
     (accept key visitor false))
  ([key ^Visitor visitor write?]
     (.accept key visitor write?)))

(defn compare-and-swap
  "##Compare and swap!

For synchronization!  Compare and swap takes a key, an old value and
a new value.  If the given old value matches what's in the DB, then
it's replaced by the new value, otherwise nothing happens.

Returns true if the swap happens, false otherwise."
  [key old new]
  (.cas db key old new))

(defn check
  "##Check

Given a key, it checks to see if it's in the database.

Returns the size of the entry if it's there, nil otherwise."
  (let [key-bytes (if (byte-array? key) key (.getBytes key))
        size? (.check db key-bytes)]
    (if (< size? 0) nil size)))

(defn clear
  "#Hoses the database!!!"
  []
  (.clear db))

(defn copy
  "##Copy

Copies the database to a destination file."
  [dest]
  (.copy db dest))

(defn dump-snapshot
  "##Dump Snapshot

Dumps the current running state of the database into a destination file."
  [dest]
  (.dump_snapshot db dest))

(defn load-snapshot
  "##Load Snapshot

Loads a previously taken snapshot into the current DB object."
  [snapshot]
  (.load_snapshot db snapshot))

(defn count
  "Gives the number of keys in the database."
  []
  (let [size? (.count db)]
    (if (< size? 0) nil size?)))

(defn match-prefix
  "Finds records whose keys begin with the given prefix. Optional
argument, `max-elts` specifies at most how many to retrieve; defaults
to no limit."
  ([prefix] (match-prefix prefix -1))
  ([prefix max-elts]
       (seq (.match_prefix db prefix max-elts))))

(defn match-regex
  "Finds records whose keys matches the given regex string.  The
regex must be a string, not a Clojure regex, otherwise it just 
won't work.  Optional argument, `max-elts` specifies at most how 
many to retrieve; defaults to no limit."
  ([rx] (match-regex rx -1))
  ([rx max-els]
     (seq (.match_regex rx max-elts))))

(defn match-similar
  "Finds records whose keys are kinda like the sample given.  This
uses the levenshtien distence, so other args are needed.

 * `origin`: the like-string we're trying to match
 * `range`: the 'distance' from our origin we're willing to match.
 * `utf-8?`: Optional, default is true.  Set to false if you don't want
   to treat keys like utf-8 strings.  I don't understand why you wouldn't
   want to...
 * `max-elts`: Optional, default is no limit.  Set to the number of result
   you want to at-most have."

  ([origin range] (match-similar origin range true -1))
  ([origin range opt]
     (if (integer? opt)
       (match-similar origin range true opt)
       (match-similar origin range opt -1)))
  ([origin range utf-8? max-elts]
     (seq (.match_similar db origin range utf-8? max-elts))))

(defn visit-everything
  "##Visitor Iteration

This sends a visitor along all the records atomically.  I don't need to
warn you that this __HUGE__ operation is deadlock Grand Central, so don't
use it if you are doing something rather involved, or have other processes
in your database."
  ([visitor] (visit-everything visitor false))
  ([visitor write?] 
     (.iterate db visitor write?)))

(defn transaction
  "##Transactions!

Transactions group reads and writes together in a way that allows you to
back out of any manipulations that could prove to be bad for your data.

 * `hard?`: an optional boolean.  Do we synchronize with the hardware 
   memory, or logically with the filesystem?  Defaults to false.
 * `op`: a thunk that represents your operation.  It must return non-nil and
   non-false if you wish to commit your actions."
  ([op] (transaction false op))
  ([hard? op]
     (.begin_transaction db hard?)
     (let [res (op)]
       (if (or (nil? res) (not res))
         (.end_transaction db false)
         (.end_transaction db true)))))

(defmacro with-db
  "## With DB Macro

This beautific sucker will handle opening up a Kyoto Cabinet database object
using the provided parameters and open-specs, bind `db` to the created 
database, and execute any forms given to it.  After all is said and done, 
it closes the database and releases all it's resources completely.

Within this form, `db` is bound the desired casket with the specified open
options, so convienent access to the object itself is still possible, in case 
you need to do something that I haven't thought of."
  [[casket & open-opts] & body]
  `(let [merged-options (->> (map #(get open-options % 0) ~open-opts)
                             (apply bit-or 0))]
     (binding [db (DB. DB/GEXCEPTIONAL)]
       (.open ~casket merged-options)
       (do ~@body)
       (.synchronize db false nil)
       (.close db))))

;; ----
;; The following are specially abastracted forms that make using Kyoto Cabinet
;; with Clojure's EDN-like data easier.  Alternatives are available if rote binary
;; needs to be stored.

(defn- edn-str
  "Takes some Clojure and writes some EDN, safely."
  [elt]
  (let [cand (pr-str elt)]
    (assert (= elt (edn/read-string cand)))
    cand))

(defn add-edn!
  "##Add

Adds a key-value pair to the database.  `key` should be a string.
or a byte array. `value` should be something that can be safely 
rendered in EDN.

Adding to a database will not overwrite an entry should it already
exist.

Returns a boolean: true if it succeeded, false if it failed."
  [key value]
  (let [edn-value (edn-str value)]
    (.add db key edn-value)))

(defn add-bin!
  "##Adding Blobs Of Stuff

Adds a key-value pair to a database.  The `key` should be a string,
and the `blob` should be a vector or array of bytes.

Returns true on success, false of failure."
  [key blob]
  (.add db (.getBytes key) 
        (if (vector? blob)
          (to-array blob)
          blob)))

(defn append-bin!
  "##Append

Appends a key-value pair to the database.  `key` should be a string.
`blob` should be a a vector or array of bytes.

Appending will either create a new entry to the DB if the key doesn't
exist, or it appends `value`'s bytes to the end of the existent entry.

Returns true on success, false on failure."
  [key blob]
  (.append db (.getBytes key)
           (if (vector? blob)
             (to-array blob)
             blob)))

(defn get-edn
  "##Get a record.

Give a key, get crunchy EDN back, or nil if it's not there."
  [key]
  (when-let [result (.get key)]
    (edn/read-string result)))

(defn get-bin
  "##More Get A Record

Give a key (must be a string), get a byte array back, or nil if it's not there."
  [key]
  (when-let [result (.get (.getBytes key))]
    result))

(defn get-bulk-bin
  "##Bulky Bins

Same thing as `get-bulk-edn`, but returns a map of key to byte array
instead."
  ([keys] (get-bulk keys true))
  ([keys atomic?]
     (let [keys (-> (map #(.getBytes %) keys)
                    to-array-2d)]
       (when-let [result (.get_bulk db keys)]
         (->> (partition 2 (seq result))
              (map (fn [[k v]]
                     [(String. k) v]))
              (reduce #(apply append %1 %2) {})))))))

(defn get-bulk-edn
  "##Get a bunch of records.

Give a collection of keys, get a key-value map of what you asked for, or
nil if something failed.  You can also specify if this will be an atomic
operation or not, the default is true if not specified."
  ([keys] (get-bulk keys true))
  ([keys atomic?]
     (-> (get-bulk-bin keys atomic?)
         (map (fn [[k v]] [k (edn/read-string (String. v))]))
         (reduce #(apply assoc %1 %2) {}))))

(defn remove-bulk!
  "##Drop A Bunch Of Blob Or A Block Of EDN

Pass in a vector of keys, and watch their entries disappear! 
If `atomic?` is false, then the operation is not atomic at all;
it defaults to true."
  ([keys] (remove-bulk! keys true))
  ([keys atomic?]
     (let [keys (-> (map #(.getBytes %) keys)
                    to-array-2d)]
       (.remove_bulk db keys atomic?))))

(defn remove!
  "Simply deletes a record from the DB.

The provided key must be a string."
  [key]
  (.remove db (.getBytes key)))

(defn replace-bin!
  "Replaces a blob with another one, by key"
  [key blob]
  (.replace db (.getBytes key) blob))

(defn replace-edn!
  "Replaces one EDN entry with another, by key"
  [key value]
  (replace-bin! key (-> (edn-str value)
                        #(.getBytes %))))

(defn sieze-bin!
  "Grab a record by key, removing it when fetched.  Returns the record."
  [key]
  (.sieze db (.getBytes key)))

(defn sieze-edn!
  "Same thing as `sieze-bin!`, but returns EDN instead."
  [key]
  (-> (sieze-bin! key)
      #(String. %)
      edn/read-string))

(defn set-bulk-bin!
  ([kv-map] (set-bulk-bin! kv-map true))
  ([kv-map atomic?]
     (let [values (->> (keys kv-map)
                       (reduce #(conj %1 (.getBytes %2) 
                                      (get kv-map %2)) [])
                       (to-array-2d))]
       (.set_bulk db values atomic?))))

(defn set-bulk-edn!
  ([kv-map] (set-bulk-edn! kv-map true))
  ([kv-map atomic?]
     (set-bulk-bin! 
      (-> (map (fn [[k v]] 
                 [k 
                  (-> (edn-str v)
                      #(.getBytes %))])
               kv-map)
          (reduce #(apply assoc %1 %2) {}))
      atomic?)))

(defn set-bin!
  ())
