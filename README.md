kafka-node-example
==================

This is a test project that makes use of the [kafka-node](https://www.npmjs.com/package/kafka-node) libraries to
write topics to a kafka server. It's using twitter as a stream source that it's writing to the server.

If you are looking for a way to publish data into a kafka topic, have a look at the ```producer.js``` file. Its counterpart is 
the ```consumer.js``` file which is an example of a topic consumer. The script simply reads the tweets off the kafka topic.

Also included, are examples of kafka consumers that subscribe to a topic of twitter data, perform some processing on
the data and then publish the results to a websocket. These microservices can then be easily consumed by a front-end dashboard.
 
Have a look at:

* device-consumer.js
* handle-consumer.js
* hashtag-consumer.js
* mood-consumer.js
* metrics-consumer.js

Getting Started
---------------

Install the required libraries via npm:

    npm install

Next, make sure you have Kafka running. The [Kakfa Quickstart](http://kafka.apache.org/quickstart)
documentation explains how to do this step-by-step.

