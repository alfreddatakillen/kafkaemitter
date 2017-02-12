kafkaemitter
============

This module provides a standard event emitter interface to Apache Kafka.


How to use it?
--------------

```javascript
const KafkaEmitter = require('kafkaemitter');
const kafka = new KafkaEmitter();

kafka.on('my-kafka-topic', data => {
	console.log(data);
});

kafka.emit('my-kafka-topic', { test: 'some data' });
```

### Waiting for acks on the `.emit()` function

Then `.emit()` function returns a Promise which will resolve when the messages
is acked by Apache Kafka. Use it this way:

```javascript
kafka.emit('my-kafka-topic', { test: 'some data' })
.this(() => {
	// This messages was acked!
})
.catch(err => {
	// We did not succeed sending this message to Apache Kafka.
});
```


Configure
---------

Pass a configration object as argument to the `KafkaEmitter` constructor,
like this:

```javascript
const KafkaEmitter = require('kafkaemitter');
const kafka = new KafkaEmitter({
	connectionString: '127.0.0.1:9092,remote-server.lan:9092'
});
```


Strict order of incoming messages
---------------------------------

This module guaratees incoming events to be emitted in strict order. Due to
JavaScript's asynchronous naturue, you might want to block new events until
your are finished processing the previous one.

You can do that by returning a Promise in the `.on()` callback.
All promises returned by `.on()` callbacks must be resolved (or rejected)
before any new events are emitted.

Example::

```javascript
kafka.on('football-events', msg => {
	// Since we return a Promise, no more events will be emitted in the
	// "football-events" topic unit the Promise is resolved.

	return new Promise((resolve, reject) => {

		doSomeAsyncDatabaseStuff(err => {
			if (err) {
				// There was an error.
				reject();
			}

			// Now process the next one!
			resolve();
		});

	});
});
```


Set offset
----------



Limitations
-----------

* This module will JSON.stringify() data before sending it to Apache Kafka,
  and it expects all incoming data to be JSON.parse()-able.
* This module was made to guarantee a strict order over records. That means
  we only support one Kafka partition. We use a simple consumer, not the
  consumer groups api.


Credits
-------

This module depends on the [kafka-node](https://github.com/SOHU-Co/kafka-node)
module which does all the Kafka heavly lifting.
