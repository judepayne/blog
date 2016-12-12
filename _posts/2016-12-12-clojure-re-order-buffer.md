---
layout: post
title:  "A re-order buffer in Clojure"
subtitle: "Part One"
date:   2016-12-12 22:00:00
category: programming
tags:
- stream processing
- transducers
- clojure
---

A re-order buffer caches incoming items and releases them in chunks according to a (usually) time based sort order.
Its role is to act as a stage in a data/ event flow to ensure that some downstream function receives events in the right order more of the time.

Here we'll talk through a couple of different implementations of a re-order buffer in Clojure - the first a bit more object orientated in style as I'd like something that exposes well through into Java code, and the second sticking more closely to the Clojure idiom and using lazy sequences.

We'll use the **clj-time** library throughout for all time based functionality. clj-time is a clojure wrapper over the widely used joda time java library.

# 1. First Design

The use-case for the re-order buffer where there is of an inbound flow of events which is (near) continuous and time sorted chunks are released only so often. The key settings of the re-order buffer are:

**hold-for**:- The least amount of time (in millis) that an item is to be held for before it can be released.

**release-every**:- The periodicity (in millis) of the release chunk operation.

**keyfn**:- A function that extracts the time based field to be used for sorting from each item.

**func**:- The function that is called on each items being released.

The various pieces needed to assemble a re-order buffer are:

-   A cache to hold the items. A re-order buffer is a simple thing and there's only one sort order which is specified upfront. Therefore, in this implementation it's not necessary to go to the complexity of a database. While a chunk of sorted items are being released, we'll block any additional updates so some synchronisation will be necessary. That synchronisation should only be for a brief amount of time, so as not to disrupt the inbound flow. Therefore we'll make the choice to sort/ index on write and choose a data structure that supports fairly fast range queries.

-   A buffer to accept inbound items, for example when a chunk is being expired from the cache and released, but this will also allow for the re-order buffer to be thread safe.

-   A scheduler to trigger the chunk release event on the time interval specified in the **release-every** parameter

## Implementation

Clojure has an appropriate abstraction for a cache of state that comes with built in synchronization - an agent. Agents come with a built in buffer, so we can use an agent to model the cache and the buffer together.
First, let's define a protocol for our Buffer. We need to be able to put items into the buffer, get it's contents and (initially) stop the scheduler described above.
We'll represent the re-order buffer itself as a Clojure record, with the necessary fields describe behaviour. The main state of the reorder buffer, the set of items currently held, will be held in a Clojure sorted-set, which is based on an immutable TreeSeq.

{% highlight clojure %}
    (defprotocol Buffer
      "Protocol for a caching buffer"
      (put [this item])
      (fetch [this])
      (stop [this]))  
    
    (defrecord
      ^{:doc "Time based Re-order(ing) buffer. holdFor is the number
         of milliseconds to hold each itme for. releaseEvery is
         the interval, in milliseconds, at which the buffer is
         flushed of items that have been held for long enough.
         f is a function to be mapped over each item being released.
         scheduluer is a java executor used to trigger the periodic
         expiry events. cache is an agent synchronised sorted-set
         used to hold the internal state."}
      ReorderBuffer [holdFor releaseEvery func scheduler cache]
      Buffer
      (put [_ item] (send cache conj item))
      (fetch [_] @cache)
      (stop [_] (.interrupt scheduler)))
{% endhighlight %}

Next, lets define a scheduler that will set up a thread that sends a function to an agent every specified number of milliseconds. For this, we'll use a java scheduled executor.

{% highlight clojure %}
    (defn send-periodically
      "At the specified interval, periodically send a Clojure
      reference type the function f"
      [ref f interval]
      (doto (Thread.
             #(try
                (while (not (.isInterrupted (Thread/currentThread)))
                  (Thread/sleep interval)
                  (send ref f))
                (catch InterruptedException _)))
        (.start)))
{% endhighlight %}

The next step is to write a comparator for the sorted-set. There's a gotcha here to be aware of; if the times of two items are equal, since it's a set, then one will be lost. So it's important to handle the equal in time case and provide another way of (randomly) comparing.

We need a function that partitions the set inside the agent into items that are to be kept and those that have 'expired' and should have the supplied function mapped over them.

First of all we need a function that will partition the set inside the agent into what is to be kept and what to be expired, and map the supplied function over what is to be expired. We may change that 'map' later to be a later bit more flexible and also for reduces as well to allow for the collection of chunks - for example rolling averages in an analytics use case.

{% highlight clojure %}
    (defn comp-fn
      "Comparator for a sorted-set"
      [keyfn]
      (fn [x y]
        (let [c (compare (keyfn x) (keyfn y))]
          (if (not= c 0)
            c
            (if (= x y)
              0
              1)))))
    
    (defn expire
      "Expire items from s which are older than hold-for and call
       f on each"
      [s hold-for f keyfn]
      (let [parts (split-with
                   #(t/before? (keyfn %)
                               (-> hold-for t/millis t/ago)) s)]
        (doall (map f (first parts)))
        (into (sorted-set-by (comp-fn keyfn)) (second parts))))
{% endhighlight %}

Almost finished! Now we need a function to act as the constructor of the re-order buffer and set up the internal members that we built earlier: the cache and scheduler.

{% highlight clojure %}
    (defn reorder-buffer
      "Creates a ReorderBuffer which hold items for hold-off and has
      expiry events every release every millis. Expired items have f
      called on them. keyfn is used to retrieve the time of each
      item"
      [keyfn hold-for release-every f]
      (let [ag (agent (sorted-set-by (comp-fn keyfn)))
            sched (send-periodically ag
                                     #(expire % hold-for f keyfn)
                                     release-every)]
        (->ReorderBuffer hold-for release-every f sched ag)))
{% endhighlight %}

Let's test the functionality..

{% highlight clojure %}
    (defn stream-of-maps
      "Function to stimulate a stream of maps of the form..
       {:t *a joda date time* :id *random int*
       :t is the upper limit for the random numnber of millis for
       how far behind now :t is set to."
      [t]
      (lazy-seq
       (Thread/sleep (rand-int 50))
       (cons {:t (t/minus (t/now) (t/millis (rand-int t)))
              :id (+ 1000 (rand-int 200))} (stream-of-maps t))))
    
    (defn make-rob [] (reorder-buffer :t 30000 5000 println))
    
    (def r (make-rob))
{% endhighlight %}

Let's have a look at the stream of data generated by stream-of-maps:

{% highlight clojure %}
    (take 10 (stream-of-maps 100))
{% endhighlight %}

results in:

{% highlight clojure %}
    ({:id 1002,
      :t #<DateTime@165210f2 2016-11-14T11:29:15.814Z>}
     {:id 1190,
      :t #<DateTime@39837479 2016-11-14T11:29:15.822Z>}
     {:id 1007,
      :t #<DateTime@6efae05a 2016-11-14T11:29:15.847Z>}
     {:id 1140,
      :t #<DateTime@68ee252d 2016-11-14T11:29:15.833Z>}
     {:id 1004,
      :t #<DateTime@6ebd6d42 2016-11-14T11:29:15.937Z>}
     {:id 1022,
      :t #<DateTime@4c3ffc50 2016-11-14T11:29:15.894Z>}
     {:id 1114,
      :t #<DateTime@75fe2dbd 2016-11-14T11:29:15.960Z>}
     {:id 1043,
      :t #<DateTime@6ca0b962 2016-11-14T11:29:15.930Z>}
     {:id 1186,
      :t #<DateTime@69f21ec9 2016-11-14T11:29:15.996Z>}
     {:id 1166,
      :t #<DateTime@5a1ca2db 2016-11-14T11:29:16.086Z>})
{% endhighlight %}

Which is not perfectly sorted.

{% highlight clojure %}
    (map #(put r %) (take 10 (stream-of-maps 25000)))
{% endhighlight %}

results in:

{% highlight clojure %}
    {:id 1156,
     :t #<DateTime@13e8b50b 2016-11-14T14:42:17.387Z>}
    {:id 1011,
     :t #<DateTime@3b5d265d 2016-11-14T14:42:18.134Z>}
    {:id 1026,
     :t #<DateTime@796401f6 2016-11-14T14:42:25.529Z>}
    {:id 1074,
     :t #<DateTime@6db8b4a7 2016-11-14T14:42:27.048Z>}
    {:id 1176,
     :t #<DateTime@74c6b01f 2016-11-14T14:42:27.464Z>}
    {:id 1118,
     :t #<DateTime@73af3a94 2016-11-14T14:42:30.008Z>}
    {:id 1198,
     :t #<DateTime@26fb0553 2016-11-14T14:42:33.734Z>}
    {:id 1118,
     :t #<DateTime@70edb6a8 2016-11-14T14:42:36.718Z>}
    {:id 1089,
     :t #<DateTime@5e63930b 2016-11-14T14:42:38.337Z>}
    {:id 1129,
     :t #<DateTime@673a8a59 2016-11-14T14:42:39.403Z>}
{% endhighlight %}

which is fully sorted.
(In the Clojure repl, the results arrive in chunks over a 30 second period).

## Improvements

There's something that I'd don't like and would want to improve on:

-   An immutable data structure, the internal sorted-set, is not an ideal data structure for something that we'd like to be performant. Since it's hidden away inside the agent, and synchronised, there's no risks if we convert it to a mutable data structure.

We'll leave this for now and show how to fix it in the functional version.

# 2. Functional version

In the functional version of the reorder buffer, we'll stick closely to the Clojure idiom of functions working over sequences. That is going to make our code a bit more sparse.
In the OO version, we built a little machine to do the job that we needed it to. In this version, that's not the case; any state is hidden inside our functions. Another key difference is that in the functional version, sequences are 'pulled through' our stateful processing functions, as opposed to being pushed into the little machine to do its job.

My first attempt (something of a deviation) is to batch the stream into time chunks.. 

{% highlight clojure %}
    (defn time-batch
      "Returns a lazy sequence of lists of items, each list holding
      all items consumed from coll in the time interval. Expected
      usage is to partition a stream/ lazy-seq into time based
      windows. Interval is specified in milliseconds."
      [interval coll]
      (lazy-seq
       (let [p (promise)
             dl (System/currentTimeMillis)]
         (future
           (deliver p
                    (doall
                     (let [stop-at (+ interval
                                      (System/currentTimeMillis))]
                       (take-while
                        (fn [_] (< (System/currentTimeMillis)
			           stop-at))
                        coll)))))
         (cons @p
	       (time-batch interval (nthrest coll (count @p)))))))
{% endhighlight %}

We do this by building a lazy sequence over the input collection, **coll**. Each item in the lazy sequence is a promise. A future is started which takes items from coll until the number of milliseconds specified in **interval** has elapsed. Then it delivers them back to the promise - so realising a chunk, and the process repeats.

{% highlight clojure %}
    ;; We need a function to test it
    (defn sim-stream
      "function to stimulate a stream"
      []
      (lazy-seq
       (Thread/sleep (rand-int 30))
       (cons (rand-int 50) (sim-stream))))
    
    ;; Test it!
    (take 4 (time-batch 200 (sim-stream)))
{% endhighlight %}

results in:

{% highlight clojure %}
    ((39 8 35 34 49 26 21 28 3 25 37)
     (13 38 15 41 9 30 0 10 15 41 30 37 13 18)
     (45 8 34 3 35 22 40 45 26 47 17 40 22 6 41)
     (27 11 9 45 23 27 43 29 15 43 18 9 14 36 9))
{% endhighlight %}

**sim-stream** is a function to simulate a stream of events generated at various points in time.
Well, it works just fine but doesn't have the functionality that a re-order buffer should.

Let's have another go - this time with a stateful transducer.

{% highlight clojure %}
    (defn reorder
      "Creates a transducer that chunks and reorders inbound items.
       Inbound items should be clojure maps. Reording is determined
       by keyfn, which specifies a key in each map. Optionally, a
       comparison function can be supplied with the keyword argument
       :comp-fn. Chunking is determined by window-fn which should
       take just one arg and is called on each inbound item. It's
       result should be a value, which items with a keyfn of less
       than, are released (as a chunk).
       A higher degree of reordering will be achieved with window
       functions that have a larger range.
       For when used as a transducer on a core.async channel, if a
       :flush signal is received, will flush contents of the cache."
      [keyfn windowfn
       & {:keys [comp-fn]
          :or {comp-fn (fn [x y]
                         (let [c (compare (keyfn x) (keyfn y))]
                           (if (not= c 0) c
                               (if (= x y) 0 1))))}}]
       (fn [rf]
         (let [cached (volatile! (sorted-set-by comp-fn))]
           (fn
             ([] (rf))                          ;; init arity
             ([result]                          ;; completion arity
              (rf result (apply list @cached))) ;; ..add leftovers
             ([result input]                    ;; reduction arity
              (if (= input :flush)
                (do
                  (let [flush (apply list @cached)]
                    (vreset! cached (sorted-set-by comp-fn))
                    (rf result flush)))
                (do (vswap! cached conj input)
                    (let [pivot {keyfn (windowfn input)}
                          ex (not-empty (subseq @cached < pivot))]
                      (if ex                    ;; add expirees
                        (do (doseq [e ex] (vswap! cached disj e))
                            (rf result ex))
                        (rf result))))))))))    ;; just pass along
    
    ;; *Some Example window functions*
    ;; for maps of the form {:t joda-time :id an integer ....}
    
    (defn time-window
      "A window function for re-ordering by time. The current item
       is ignored. Items with a :t (in example maps) of more than
       window milliseconds before prior will be released."
      [window]
      (fn [_] (t/minus (t/now) (t/millis window))))
    
    (defn id-win
      "A window function for re-ordering by :id (in example maps).
       Will cause any cached items with an id than is *window* less
       than the id of the current item to be released."
      [window]
      (fn [item] (- (:id item) window)))
{% endhighlight %}

This is worth going through step by step.

{% highlight clojure %}
    {comp-fn (fn [x y]
               (let [c (compare (keyfn x) (keyfn y))]
                 (if (not= c 0) c
                     (if (= x y) 0 1))))}
{% endhighlight %}

This is where we set up a default comparison function that controls the internal order of sorting amongst items cached inside the transdcuer.

Transducers are worth spending a little time on, although there's a wealth of material on the web for deeper explanations. The pattern for a transducer function looks something like this..

{% highlight clojure %}
    (defn my-transducer
      [*some args*]
       (fn [rf]  ;; <- rf means 'reducing function'
         (let [my-cache (*set up internal state here*)]
           (fn
             ([] (rf))                          ;; init arity
             ([result] (rf result))             ;; completion arity
             ([result input]                    ;; reduction arity
              (..*do something with the input, result &
                     pass along into the next result here*))))))
{% endhighlight %}

A transducer returns a function that itself takes a reducing function. e.g. in the reduce call that may be more familiar

{% highlight clojure %}
    (reduce + [1 2 3 4])
{% endhighlight %}

'+' is the reducing function. Rather than '+' for our reorder transducer we'll use 'conj' as we'll see later on.
Inside the inner function, there is a clear strucutre for transducers. First we set up some internal store of state if required; not all transducers need be stateful.
Next there is a function that has three arities, known as the init arity, the completion arity and the reduction arity. The simplest way I found to think of these is as 'handlers' for the three scenarios that a transducer can be used for while processing a sequence: init - before the first element is processed when we just have the rf available to us, at the termination of the sequence, in case there is some clean-up to do, as there is in re-order when we flush any items left in the cache and conj them onto the end of the output sequence. Finally, in the reduction arity there is where you would write your standard function that handles the result (or 'accumulator' of the reduction so far) and the (new) input and outputs the result to be used in the next step.

Now that the scaffolding that a transducer comes with is clearer, the rest of the code is pretty easy to make out.

1.  When a new item comes in it's cached.
2.  Then the result of the window-fn is checked and any cached items that meet the criteria for expiry are tacked onto the end of the created/ output sequence (and flushed from the cache).
3.  Finally when the incoming sequence has terminated, then the remaining items in the cache are tacked onto the end of the output sequence.
4.  The cache in this is sorted by the **keyfn** supplied - this makes the range/ **subseq** style selections to pick up items to be expired performant.

So, a transducer is a functions that takes in a reducing function, e.g. 'conj' and outputs another, in our case considerably more complex, reducing function - and so on.
Transducers therefore have a couple of nice advantages over the standard Clojure idiom of mapping, reducing and filtering over sequences:

-   Transducers are separate from the sequence that they are then applied to. They can work over any sequence, including a core.async channel and according to that, the evalutation style can be lazy or eager; that can be determined separately from the transducer.

-   Multiple transducers are chained together to make one, final reducing function .. which is then applied over the target sequence. This is as opposed to applying a function to a collection, getting a new collection out, then applying the next function over that, and so on &#x2026;   This means that for any serious work that transducers, without the need to have intermediate collections created, are much more performant.

Transducers might not phase a Haskeller, but the Clojure community justifyibly thinks they're pretty cool!

Let's test it!

{% highlight clojure %}
    ;; let test by applying the transducer over a sequence
    (transduce
     (reorder :t (time-window 100))
     conj
     (take 8 (stream-of-maps 100)))
{% endhighlight %}

results in:

{% highlight clojure %}
    [({:id 1098,
      :t #<DateTime@6f7e7c56 2016-11-14T13:55:25.883Z>}
      {:id 1074,
      :t #<DateTime@1192e3f9 2016-11-14T13:55:25.901Z>})
     ({:id 1051,
       :t #<DateTime@624d052a 2016-11-14T13:55:25.919Z>})
     ({:id 1155,
       :t #<DateTime@15e45166 2016-11-14T13:55:25.968Z>})
     ({:id 1046,
       :t #<DateTime@2ba0b95b 2016-11-14T13:55:25.995Z>}
      {:id 1126,
       :t #<DateTime@67f289ca 2016-11-14T13:55:26.012Z>})
     ({:id 1193,
       :t #<DateTime@678086d9 2016-11-14T13:55:26.069Z>}
      {:id 1027,
       :t #<DateTime@5058942e 2016-11-14T13:55:26.122Z>})]
{% endhighlight %}

As you can see, the reorder-buffer results in a time sorting that is much better than the input sequence generated by sim-stream2. The larger the time window, the better that we could expect the buffer to perform. In real-world usage, you'd tune the time window to the out-of-order-ness of the incoming stream and the requirements that the next step in the processing chain has to receive the data quickly enough.
Notice that the results arrive in chunks - e.g. an initial chunk of 2, then a 1 etc &#x2026;

Since we'd expect to use a re-order buffer in over a real stream, there's one final test to do, which is to put it over a core.async channel and see how it works.

{% highlight clojure %}
    ;; create a core.async channel
    (def c (a/chan 10 (reorder :t (time-window 200))))


    ;; set up a go block to print each item received
    (go (loop [n 0]
          (if (< n 50)
            (do
              (println (a/<! c))
              (recur (inc n)))
            (a/close! c))))
    
    ;; place a number of random maps onto the channel
    (a/onto-chan c (take 8 (stream-of-maps 100)) false)
    
    ;; results =>
    ({:id 1117, :t #<DateTime@16b14ab1 2016-11-19T22:37:59.602Z>})
    ;; one was flushed out
    
    ;; let's test :flush
    (a/>!! c :flush)
    
    ;; results =>
    ({:id 1151,
      :t #<DateTime@c00da66 2016-11-19T22:37:59.658Z>}
     {:id 1035,
      :t #<DateTime@10c977c3 2016-11-19T22:37:59.666Z>}
     {:id 1013,
      :t #<DateTime@74016064 2016-11-19T22:37:59.687Z>}
     {:id 1193,
      :t #<DateTime@410d6c28 2016-11-19T22:37:59.708Z>}
     {:id 1195,
      :t #<DateTime@4bd45d85 2016-11-19T22:37:59.717Z>}
     {:id 1109,
      :t #<DateTime@10f84c53 2016-11-19T22:37:59.747Z>}
     {:id 1013,
      :t #<DateTime@6f050c68 2016-11-19T22:37:59.818Z>})
    ;; and the rest
{% endhighlight %}