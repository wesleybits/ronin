(defproject ronin "0.1.0a"
  :description "Idiomatic wrapper for Kyoto Cabinet"
  :url "https://github.com/wesleybits/ronin"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-marginalia "0.7.1"]]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [com.fallabs/kyotocabinet-java "1.24"]]
  :source-paths ["src""src/clojure"]
  :java-source-paths ["src/java"])
