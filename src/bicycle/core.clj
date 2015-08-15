(ns bicycle.core
  (require [clojure.string :as str]
           [incanter.core :as incanter]
           [incanter.charts :as charts]
           [incanter.io :as io]
           [clojure.data.csv :refer :all]
           [clj-time.coerce :as c]
           [clj-time.core :as t]
           [clj-time.predicates :as cp]))

; pure general utility functions
(defn update-where
  [find-fn update-fn vec]
  (map (fn [x] (if (find-fn x)
                 (update-fn x)
                 x)
         vec)))

(defn rolling-average 
  ([v f]
   (loop [arr (next v) res [(first v)]]
     (if (empty? arr)
       res
       (recur (next arr)
              (conj res (f (peek res) (first arr)))))))
  ([arr]
   (rolling-average arr (fn [m x]
                          (+ (* 0.02 m)
                             (* 0.98 x))))))

(defn group-by-filter [m data]
  (apply merge (map (fn [[key f]]
                      {key (filter #(f (first %)) data)}) 
                    m)))

(defn all? [f arr]
  (loop [arr arr]
    (if (empty? arr)
      true
      (and (f (first arr)) (recur (next arr))))))

(defn map-h [f m]
  (into {} (map f m)))

(defn smart-pop [arr]
  (if (empty? arr)
    []
    (pop arr)))

(defn push-to-last-vector [arr entry]
  (conj (smart-pop arr) 
        (conj (or (peek arr) []) entry)))

(defn partition-to-segments 
  "(partition-to-segments #(= (mod % 5) 0) [ 1 2 3 4 5 6 7 8 9 10])
  => [ [ 1 2 3 4 ] [ 5 6 7 8 9 ] [ 10 ] ]"
  [f arr]
  (reduce (fn [m entry]
            (cond
              (f entry) (conj m [entry])
              :else (push-to-last-vector m entry))) [] arr))

(defn transpose 
  ([matrix] (transpose matrix (count (first matrix)))) 
  ([matrix length]
    (map (fn [n] (map #(nth % n) matrix)) (range length))))

(defn nth-portion [arr n]
  (/ (nth arr n) (apply + arr)))

(defn portionize 
  ([arr] 
   (let [sum (apply + arr)]
     (map #(/ % sum) arr))))

(defn reduce-index
  ([f val arr]
   (reduce (fn [memo [idx x]]
             (f memo idx x)) 
           val (map-indexed list arr))))

(defn make-partitioner [filter-fn partition-fn]
  (fn [f data]
    (let [partitioned (filter filter-fn (partition-to-segments partition-fn data))]
      (map f partitioned))))

; domain specific utility functions (still pure) 

(defn to-epoch [x]
  (let [[month day year] (map #(Integer/parseInt %) (str/split x #"/"))
        year (+ 2000 year)]
    (c/to-long (t/date-time year month day))))

(defn first-day-of-month? [x]
  (cp/first-day-of-month? (c/from-long x)))

(defn first-day-of-year? [x]
  (let [obj (c/from-long x)]
    (= (.getDayOfYear obj) 1)))

(defn monday? [x]
  (cp/monday? (c/from-long x)))

(defn tuesday? [x]
  (cp/tuesday? (c/from-long x)))

(defn wednesday? [x]
  (cp/wednesday? (c/from-long x)))

(defn thursday? [x]
  (cp/thursday? (c/from-long x)))

(defn friday? [x]
  (cp/friday? (c/from-long x)))

(defn saturday? [x]
  (cp/saturday? (c/from-long x)))

(defn sunday? [x]
  (cp/sunday? (c/from-long x)))

(defn day-of-week [x]
  (nth ["Sunday" "Monday" "Tuesday" "Wednesday" 
        "Thursday" "Friday" "Saturday"]
       x))

(defn month-from-integer [x]
  (nth ["Jan" "Feb" "Mac" "Apr" "May" "Jun"
        "Jul" "Aug" "Sep" "Oct" "Nov" "Dec"] x))

(defn- data-vectors-to-hash [v]
  {:timestamp (first (first v))
   :data (map second v)})

; impure data functions 

(defn raw-data []
  (with-open [in-file (clojure.java.io/reader "data.csv")]
    (doall (read-csv in-file))))

(def data 
  (map (fn [row]
         [(to-epoch (first row)) 
          (Integer/parseInt (str/replace (second row) #"," ""))]) 
       (raw-data)))

(defn data-by-day-of-week []
  (map-h (fn [[key f]]
             [key (filter f data)]) 
         (map-h (fn [[key f]]
                    [key #(f (first %))]) 
                {:monday monday?
                 :tuesday tuesday?
                 :wednesday wednesday?
                 :thursday thursday?
                 :friday friday?
                 :saturday saturday?
                 :sunday sunday?})))

(defn total-by-day-of-week []
  (map-h (fn [[key entries]]
               [key (reduce #(+ (second %2) %1) 0 entries)])
             (data-by-day-of-week)))

(defn data-by-month []
  ((make-partitioner #(>= (count %) 28) 
                     #(first-day-of-month? (first %)))
   (fn [v] 
     (let [timestamp (first (first v))]
       {:timestamp timestamp
        :data (map second v)
        :month (-> (c/from-long timestamp) (.getMonthOfYear) (dec))   
        :year (.getYear (c/from-long timestamp))}))
   data))

(defn data-by-year 
  ([] (data-by-year #(>= (count %) 365)))
  ([filter-fn]
  ((make-partitioner filter-fn 
                     #(first-day-of-year? (first %)))
   (fn [v] 
     (let [timestamp (first (first v))]
       {:timestamp timestamp
        :data (map second v)
        :year (.getYear (c/from-long timestamp))}))
   data)))

(defn data-by-week []
  (let [weekly-data (filter #(= (count %) 7) 
                            (partition-to-segments #(sunday? (first %)) 
                                                   data))]
    (map (fn [x] {:timestamp (first (first x))
                  :data (map second x)}) weekly-data)))


; impure plotting functions

(defn plot-month-series 
  ([] (plot-month-series (range 12)))
  ([months-to-include]
  (let [monthly (map #(assoc % :data (apply + (:data %))) (bicycle.core/data-by-month))]
    (incanter/view 
      (reduce-index
        (fn [c idx [month x y]] (charts/add-lines c x y
                                            :series-label month))
        (charts/time-series-plot (map :timestamp monthly)
                                 (map :data monthly)
                                 :title "Number of bicycle hires by month"
                                 :x-label "month"
                                 :y-label "Number of hires"
                                 :series-label "Main Line"
                                 :legend true) 
        (map (fn [n]
               (let [month-data (filter #(= (:month %) n) monthly)]
                 [(month-from-integer n)
                  (map :timestamp month-data) 
                  (map :data month-data)])) 
             months-to-include))))))

(defn plot-rolling-average []
  (let [x (map first data)
        y (rolling-average (map second data))]
    (incanter/view (charts/time-series-plot x y 
                                            :title "Rolling average"
                                            :x-label "datetime"
                                            :y-label "Number of hires"))))

(defn plot-time-series []
  (incanter/view (charts/time-series-plot
                   (map first data)
                   (map second data)
                   :title "Number of bicycle hires by day"
                   :x-label "day"
                   :y-label "Number of cycle hires")))

(defn plot-by-day-of-week
  ([] (plot-by-day-of-week false))
  ([with-points?] 
   (let [data (data-by-week)
         x (map :timestamp data)
         ys (transpose (map #(portionize (:data %)) data))  
         chart (charts/time-series-plot 
                 x (first ys)
                 :title (str "Proportion of bicycle hires by week" )
                 :x-label "day"
                 :y-label "Number of hires"
                 :series-label (day-of-week 0)
                 :legend true)]
     (incanter/view (reduce-index (fn [c idx y] 
                                    (charts/add-lines c x y
                                                      :points
                                                      with-points?
                                                      :series-label
                                                      (day-of-week (inc idx)))) 
                                  chart (next ys))))))

(defn plot-days-histogram []
  (let [m (total-by-day-of-week)]
    (incanter/view (charts/bar-chart (keys m)
                                     (map second m)))))
; asseting functions
(defn check-all-days-exists []
  (let [timestamps (map first data)]
    (assert (all? #(= % 86400000) (map - (next timestamps) timestamps)))
    (println "All days do exist in dataset!")))


