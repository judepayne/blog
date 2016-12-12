---
layout: post
title:  "Introduction to stream processors"
subtitle: "Part One"
date:   2016-12-08 21:46:04
category: programming
tags:
- stream processing
- stream processor
- clojure
- apache spark
---

Processing data in live streams is a very exciting area of technology at the moment. Given the trend to prominence, it's quite likely that in a few years time 'Stream Processors' will be as well understood a class of application as 'Database' or 'Web Server' are today.

But stream processing is nothing new. At its heart stream processing is just a way of arranging/ thinking about how to arrange your compute. The stream processing way of doing it meets the modern need of having for processing events as soon as they are received. There's also a clear tie in to functional programming; stream processing is actually one of the core paradigms in functional languages which process functions over streams/ collections of data. 

In this article, I'll explain stream processors by looking at the various pieces that are needed to make one up and show how production grade stream processors (e.g. [Apache Spark](http://spark.apache.org)) build on that to achieve resilient, scalable processing.


## A toy stream processor

To illustrate what a stream processor is, let’s build a very basic example. Although basic, it will have nearly all the same parts as it’s production grade counterpart like Apache Spark, Apache Samza or Axiom. We’ll use Clojure, a functional lisp for the JVM as it’s very efficient to get stuff done in. As we'll expand upon later, one of the foundational idioms in functional programming language is processing streams, or rather collections; Functional languages are a great fit for constructing stream processors.

Stream processors listen to one or more streams of data that might be provided from message buses, web sockets or even database pollers. Our example will come from the world of Finance and will listen to stream of 'Orders' and 'Executions' and produce 'Trades' from them. The first job is to mock up a couple of streams of data…

{% highlight clojure %}
    (ns stream-processor.part1)
    
    (defn order-fountain []  
    (lazy-seq  
      (cons  
            {:comp-id (+ 1000 (inc (rand-int 10)))  
             :type 'order'  
             :ord-attr (['red' 'yellow' 'blue'] (rand-int 3))}  
            (order-fountain))))
{% endhighlight %}

First we declare a namespace of our stream processor's code to sit in. The `order-fountain` function produces a endless stream of randomised orders. Each order is a Clojure hashmap with keys :comp-id (an identifier), :type and :ord-attr which contains the actual attributes associated with the order ("red", "yellow" or "blue" in our toy stream processor app). The way the function works is to produce a random order and '`cons`' it onto another call to itself. That is then wrapped in a `lazy-seq` to make it lazy and not immediately process forever when called; only those items that are asked for will be processed.

We can use it like this;
{% highlight clojure %}
    (take 5 (order-fountain))
{% endhighlight %}

results in table form:

<div class="table-wrapper">
<table>
<tbody>
<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1004</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1004</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1003</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">yellow</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1004</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">blue</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1008</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">blue</td>
</tr>
</tbody>
</table>
</div>

We get a sequence of five orders from our fountain with randomised elements. Let’s build something a similar for a sequence of executions…

{% highlight clojure %}
    (defn execution-fountain []  
      (lazy-seq  
        (cons  
          {:comp-id (+ 1000 (inc (rand-int 7)))  
           :type 'execution'  
           :exec-attr ([:x :y :z] (rand-int 3))}  
          (execution-fountain))))
{% endhighlight %}

Now let’s combine those two streams into one as follows and check the output when we take the first few items off the resulting stream…
{% highlight clojure %}
    (->
      (take 10
      (interleave (order-fountain) (execution-fountain))))
{% endhighlight %}

<div class="table-wrapper">
<table>
<colgroup>
<col  class="org-left">
<col  class="org-right">
<col  class="org-left">
<col  class="org-left">
<col  class="org-left">
<col  class="org-left">
</colgroup>
<tbody>
<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1006</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">blue</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1002</td>
<td class="org-left">:type</td>
<td class="org-left">execution</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:z</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1004</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">blue</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1005</td>
<td class="org-left">:type</td>
<td class="org-left">execution</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:y</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1003</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1004</td>
<td class="org-left">:type</td>
<td class="org-left">execution</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:z</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1007</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1006</td>
<td class="org-left">:type</td>
<td class="org-left">execution</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:x</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1008</td>
<td class="org-left">:type</td>
<td class="org-left">order</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">yellow</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1002</td>
<td class="org-left">:type</td>
<td class="org-left">execution</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:x</td>
</tr>
</tbody>
</table>
</div>

`->` is the threading macro - it sequences a series of operations.

As you can see, we get an order then execution then order.. etc. That's what `interleave` does. Good enough for our dummy stream. Both orders and executions have a 'comp-id' which is randomised to between 1001 and 1010. That’s what we’ll be joining on - when an order and a trade have the same comp-id, we want to make them into a trade. An execution has an `:exec-attr` field rather than an `:ord-attr`. It can take values :x, :y or :z.

A stream processor always has some sort of stateful store. It might be a sql database, a distributed key value store or something else. This components provides state and is also one of the components that provides resilience (We'll come to the other component needed for resilience later).

In this example we’ll use the simplest store of all - a hash map. `{}` in Clojure.

{% highlight clojure %}
    (def stateful-store (atom {}))
{% endhighlight %}

Clojure, as a functional langugage, tries to operate without explicit state as much as possible, but sometimes you need it. Our hash map store is placed inside an `atom` which ensures that any changes made to the hash map are atomic and thread-safe.
Now let’s get to the main event, the stream processor itself.

We’ll write a ‘trade maker’ function takes in a stream of data and takes in a join function that is used to join an order and execution with matching comp-id’s (our pivot field). In this case the join function will simply merge the hash map representing the order with that of the stream and set the :type of the result to ‘trade’…

{% highlight clojure %}
    (def my-join [i1 i2] (assoc (merge i1 i2) :type “trade”))
{% endhighlight%}

The logic of the `make-trade` function that we need next is simple.

First we extract the comp-id from the trade or order in hand and check if its counterpart is already in our stateful store.

If not, then we need to cache the item in hand. If is is, then we apply the join function to the item in hand and the retrieved cached item, thereby joining execution to order or order to execution…

{% highlight clojure %}
    (defn make-trade [stream join-fn]  
      (map  
       (fn [item]  
         (let [comp-id (:comp-id item)  
               other-side (get @stateful-store comp-id)]  
           (if (or (nil? other-side)
                   (= (:type item) (:type other-side)))  
             (do (swap! stateful-store assoc comp-id item)  
                 (str 'caching ' (:type item) ' comp-id: ' comp-id))  
             (if (not= (:type item) (:type other-side))  
               (do (swap! stateful-store dissoc comp-id)  
                   (join-fn item other-side))))))  
       stream))
{% endhighlight %}

Ok, we’re done!

Let’s test it out…

{% highlight clojure %}
    (->
      (take 25
      (interleave (order-fountain) (execution-fountain)))  
      (make-trade my-join))

    ("caching order comp-id: 1004"
     "caching execution comp-id: 1002"
     "caching order comp-id: 1005"
     "caching execution comp-id: 1001"
     "caching order comp-id: 1010"
     {:comp-id 1005, :type "trade", :exec-attr :x, :ord-attr "blue"}
     "caching order comp-id: 1010"
     {:comp-id 1004, :type "trade", :exec-attr :z, :ord-attr "yellow"}
     {:comp-id 1001, :type "trade", :ord-attr "blue", :exec-attr :x}
     "caching execution comp-id: 1007"
     "caching order comp-id: 1001"
     {:comp-id 1001, :type "trade", :exec-attr :x, :ord-attr "red"}
     "caching order comp-id: 1009"
     "caching execution comp-id: 1007"
     "caching order comp-id: 1010"
     "caching execution comp-id: 1003"
     "caching order comp-id: 1001"
     "caching execution comp-id: 1004"
     {:comp-id 1004, :type "trade", :ord-attr "yellow", :exec-attr :y}
     "caching execution comp-id: 1005"
     {:comp-id 1007, :type "trade", :ord-attr "red", :exec-attr :y}
     "caching execution comp-id: 1007"
     "caching order comp-id: 1001"
     "caching execution comp-id: 1004"
     "caching order comp-id: 1009")
{% endhighlight%}

As you can see, the sequence produced is a mix of events where there is no match and the event is cached. When a match occurs, a trade is output with both order attributes and execution attributes.

Here’s the topology of the toy processor:

![](../../images/toy1.svg)

All stream processors arrange their calculations in a topology, or ‘graph’. To describe any topology, we only need four relationships between nodes in the toplogy:

-   **Combination**: two of more nodes point at the same node.

-   **Chain**: one node points to the next.

-   **Split**: a node points at the two or more nodes.

-   **Many to many**: multiple incoming and out going nodes.

There are only four types of computations which can happen at each node. We’ve seen two of these types already.

Mappers ‘map’ a function over each element in the stream. sources and sinks are special cases of mappers - the endpoints of a topology. The `make-trade` function is an example of a mapper. Mappers always occur at a chain node - they are a single step in our topology.

Combiners combine two or more streams with some logic. The expression `interleave (order-fountain) (execution-fountain)` is an example of a combiner. Combiners only occur at combination nodes.

To illustrate the other two types of computation, we’ll extend the toy a little by making a new function to add to the processing topology:

{% highlight clojure %}
    (defn filter-colour [stream colour]
      (filter #(= colour (:ord-attr %)) stream))
{% endhighlight %}

Filterers/ patitioners filter or split a stream. The `filter-colour` function which filters only those trades which have an `:ord-attr` field that matches the supplied colour is an example of a filterer/ partitioner. A filterer/ partitioner can only ever occur at a Split type of the node in the graph. A filterer is actually a special case of a partitioner - it’s a partitioner that only produces one output stream, the filtered out items being directly dropped rather than directed into another stream.

We can add it into our processing chain like this. Note that this time, 100 items are taken off the fountain.

{% highlight clojure %}
    (-> (take 100
        (interleave (order-fountain) (execution-fountain)))  
        (make-trade my-join)
        (filter-colour "red"))
{% endhighlight %}

<div class="table-wrapper">
<table>
<tbody>
<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1007</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:y</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1004</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:y</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1003</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:x</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1006</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:x</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1001</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:y</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1005</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:x</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1002</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:y</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1005</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:y</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1004</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:z</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1004</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:z</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1007</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:y</td>
</tr>


<tr>
<td class="org-left">:comp-id</td>
<td class="org-right">1006</td>
<td class="org-left">:type</td>
<td class="org-left">trade</td>
<td class="org-left">:ord-attr</td>
<td class="org-left">red</td>
<td class="org-left">:exec-attr</td>
<td class="org-left">:z</td>
</tr>
</tbody>
</table>
</div>

Reducers. The final type of computation is a reducer, the most powerful type. All of the first three types of computations can be expressed in terms of a reducing function. A reducer takes the first item of the stream, applies the given function it to get a result. It then takes the next item and the previous result, applies the function again to get the next result, and so on… In this way, reducers ‘collect up’ the stream. Average price sliding windows are a good example of a reducer. Reducers can occur at type of node in the toplogy.

To illustrate reducers, let’s use one of the simplest: `count` this time taking 20,000 items off the fountain…

{% highlight clojure %}
    (->
     (take 20000 (interleave (order-fountain) (execution-fountain)))
     (make-trade my-join)
     (filter-colour "red")
     count)

    1850
{% endhighlight %}

(note: Every time the above expression is evaluated, we’d get a slightly different result due to the randomisation of orders and executions in the fountain.)

Here’s the final toplogy of the toy:

![](../../images/toy2.svg)

What would be our next steps in making this less toy-like?

We could change the dummy order and execution-fountain functions to, say, listen to channels on a message bus. None of the other code would need to change.

The stateful store provides one half of the resilience story for the system. We could replace the hashmap with, say, a replicated Key-Value store and our system is more resilient.

The output stream of trades rather than being printed out would go somewhere; e.g. to a database or into another channel of a message bus.

Wrap all functions into higher order functions to provide consistent logging + error handling etc.

Provide the other half of the resilience story, **the event log**. Every event coming into the processor must immediately be logged so that the processor can recover its state later if it crashes with it is half way through processing an event through it's topology.

Finally, we've made one giant simplification in our toy processor which is that there are no branches in the processing logic. In the example, we used a filter operation but not a full-blown partition operation. Having two branches of processing would have forced us to examine whether we want to be able to process multiple events through the graph concurrent, imagining each of the processing steps as nodes with asynchronous communication between them. Such a set allows for greater efficiency but brings further complexity into play when thinking about resilience. Rather than a processor that processes events one at a time and has either processed a particular event or it hasn't, we could have multiple events at various stages of processing inside our graph. Saying exactly what the state of that graph is in the event of failure so that we can resume later becomes much harder. A topic of a future blog.

## Production grade stream processors

With this understanding of the basic generalities of a stream processor, we can see that there are stream processors and stream processor use cases all around.

Where we need to process a sequence of events, we're effectively building a stream processor application. Stream processing is sometimes mistakenly conflated with the need to process big data in stream, but smaller scale stream processor use cases are just as valid.

Production grade stream processors fall into two camps: distributed or non-distributed.

Distributed stream processors are a hot area of technology right now and are seen as companion-successors to Hadoop for large scale data processing. Hadoop for massive offline batch processing and stream processors for immediate/ ‘less deep’ processing on the incoming stream of data with output of the two combining in a very large data store with both deep and immediate data (analytics) in it. This is known as a 'Lambda architecture', while the streaming part of it on its own a 'Kappa architecture'

In my experience, non-distributed stream processors are rarely called as such - rather people often talk about certain patterns (e.g. 'event sourcing') or CEP 'complex event processing'. As mentioned earlier, when the topology of the graph you need becomes complex enough that asychronous communication between the (processing) nodes is required, failure recovery is a challenge unless it's ok to lose a certain amount of data as for example is certain analytics use cases. Therefore, in order to take that complexity away from the developer who wishes to focus on the task in hand, there is definitely a role for small scale stream processing frameworks.

Building a distributed stream processor is considerably harder than a non. Difficult scenarios like node failure during processing an event, another node taking over processing the event and then the first node coming back up and this time successfully processing the event - with duplicating the output - have to be solved for. The major distriubted stream processors are all therefore large Apache projects with hundreds of committers each. The major ones are: Apache Spark, Apache Storm, Apache Flink and Apache Samza.

These distributed stream processors are all basically fairly similar in their main concepts but make slightly different choices about whether they process one event at a time (Storm, Flink and Samza) or in micro-batches (Spark), whether they guarantee to process an event exactly once or at least once, what types of resilient stores they offer.. etc.

Let’s have a brief look at Apache Spark, the most popular of the big four, to see what kinds of advantages it offers over a home grown framework.

The first area of advantage is clearly scale. Spark is a medium latency, high throughput framework that scales pretty much linearly with the number of nodes added. Spark runs in an all-active cluster which can span over datacentres. If nodes/ a data centre is suddenly unavilable, processing will continue albeit probably more slowly until full capacity is restored. The benefit of scale is that a Spark cluster can run many topologies at once; freeing up multiple teams with similar stream processing needs to focus more on their business logic rather than the non functionals.

The second area where Spark or the other mature stream processor frameworks really shines over home grown frameworks is convenience. This point is probably at least as important as the first.

To define a processing topology in Spark, you would:

*  Define the toplogy of the graph (a list of nodes and their relationships).

*  Define the type of each node: mapper, partitioner, etc.

*  Define the business logic that runs in each node.

By working with the general concepts and having all the scaffolding done for you, new computations are quick to define.

Spark’s resilient store is called ‘RDD’ which stands for ‘Resilient Distributed Dataset(s)’. These are effectively like relational database tables and the developer works with them in Java or Scala code which is very like SQL, helping transition.

Finally, on convenience, Spark offers a wide range of end-points out of the box. It can connect to JMS queues, has database pollers, file watchers, RMDB sysnchronisers etc - which strip out a lot of the 'plumbing in' type work.

I hope that served is a useful introduction to basics of stream processors and current state of the market. It’s an exciting and relatively new area which can have a number of important applications in any organisation that deals with a lot of live data. By recognising stream processing use cases, it's easier to think of applications which might be performing very different business logic as being essentially the same as each other.