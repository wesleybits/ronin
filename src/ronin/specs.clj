;; Specification maps used by much of Ronin.

(ns ronin.specs
  (:import [kyotocabinet DB]))

(def ^{:doc "A lookup table for Kyoto Cabinet's DB:open options.

 * `:sync`: auto synchronization, saves changes to the DB file.
 * `:transaction`: auto transaction, won't synchronize if there's a problem.
 * `:create`: create a casket (DB file), if it doesn't exist.
 * `:no-lock`: non-locking reads and writes, use with extreme care!
 * `:no-repair`: don't fix the DB if something goes wrong, use with care!
 * `:read`: read mode
 * `:write`: write mode
 * `:try-lock`: locking reads and writes. Safe but slow.
 * `:truncate`: open with data truncation. Improves speed, but screws with your data."}
  open-options
  {:sync        DB/OAUTOSYNC
   :transaction DB/OAUTOTRAN
   :create      DB/OCREATE
   :no-lock     DB/ONOLOCK
   :no-repair   DB/ONOREPAIR
   :read        DB/OREADER
   :truncate    DB/OTRUNCATE
   :try-lock    DB/OTRYLOCK
   :write       DB/OWRITER})

(def ^{:doc "A lookup table for database types.

These options will ignore the filename, since the DB will be kept
in memory.

 * `:prototype-hash`: An in-memory prototype hash DB.
 * `:prototype-tree`: An in-memory prototype tree DB.
 * `:stash`:          An in-memory stash DB.
 * `:cache-hash`:     An in-memory cache hash DB.
 * `:cache-tree`:     An in-memory cache tree DB.

The following options will modify the file path to match the expected file
extention to match the expected values covered in 
[Kyoto Cabinet's DB.open() docuementation](http://fallabs.com/kyotocabinet/javadoc/kyotocabinet/DB.html)

 * `:file-hash`: a persistent file hash DB.
 * `:file-tree`: a persistent file tree DB.
 * `:directory-hash`: a persistent directory hash DB.
 * `:directory-tree`: a persistent directory tree DB.
 * `:plain-text`: a plain text database."}
  db-specs
  {:prototype-hash "-"
   :prototype-tree "+"
   :stash ":"
   :cache-hash "*"
   :cache-tree "%"
   :file-hash ".kch"
   :file-tree ".kct"
   :directory-hash ".kcd"
   :directory-tree ".kcf"
   :plain-text ".kcx"})

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
