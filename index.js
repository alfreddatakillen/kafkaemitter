const kafka = require('kafka-node');

class KafkaEmitter {

	constructor(opts) {
		if (typeof opts !== 'object') {
			opts = {};
		}
		this._config = {
			clientId: opts.clientId || 'instance-' + Math.random().toString(36).substr(2, 7),
			connectionString: opts.connectionString || null,
			requireAcks: opts.requireAcks || 1,
			compression: 1
		};

		this._listeners = {};
	}

	_addIncomingMessage(topic, msg) {
		try {
			msg = JSON.parse(msg);
		} catch (err) {
			return false;
		}
		if (typeof this._receiveBuffer[topic] === 'undefined') {
			return false;
		}
		this._receiveBuffer[topic].push(msg);
		setImmediate(() => {
			this._processIncoming(topic);
		})
		return true;
	}

	_initClient() {
		if (typeof this._client === 'undefined') {
			if (!this._config.connectionString) {
				// If we have no connectionString,
				// then mock the kafka-node module.
				this._client = {
				};
			} else {
				this._client = new kafka.Client(
					this._config.connectionString,
					this._config.clientId,
					{ sessionTimeout: 60000, spinDelay: 1000, retries: 60 }, // ZooKeeper options
					{ noAckBatchSize: null, noAckBatchAge: null },
					{ rejectUnauthorized: false } // ssl options
				);
			}
		}
		return this._client;
	}

	_initConsumer() {
		let client = this._initClient();
	}

	_initProducer() {
		let client = this._initClient();
	}

	_processIncoming(topic) {
	}

	emit(topic, data) {
		// Important that stringifying happens immediately, for immutabiliy.
		data = JSON.stringify(data);

		if (typeof this._sendBuffer === 'undefined') {
			this._sendBuffer = {};

			// This is the first message.
			this._initProducer();
		}

		if (typeof this._sendBuffer[topic] === 'undefined') {
			// First message sent to this topic.
			this._sendBuffer[topic] = [ data ]

		} else {
			this._sendBuffer[topic].push(data);
		}
	}

	eventNames() {
		return Object.keys(this._listeners).sort();
	}

	listenerCount(topic) {
		return this.listeners(topic).length;
	}

	listeners(topic) {
		if (typeof this._listeners[topic] === 'undefined') {
			return [];
		}
		return [... this._listeners[topic]];
	}

	on(topic, callback) {
		if (typeof this._receiveBuffer === 'undefined') {
			this._receiveBuffer = {};

			// This is the first listener.
			this._initConsumer();
		}

		if (typeof this._receiveBuffer[topic] === 'undefined') {
			// This is the first message to this topic.
			this._receiveBuffer[topic] = [];
		}

		if (typeof this._listeners[topic] === 'undefined') {
			this._listeners[topic] = [];
		}
		if (this._listeners[topic].indexOf(callback) === -1) {
			this._listeners[topic].push(callback);
		}

	}

	once(topic, callback) {
		let wrappedCallback = data => {
			this.removeListener(topic, wrappedCallback);
			return callback(data);
		};
		this.on(topic, wrappedCallback);
	}

	removeAllListeners(topic) {
		if (typeof topic === 'undefined') {
			this.eventNames().forEach(topic => {
				this.removeAllListeners(topic);
			});
		}
		this.listeners(topic).forEach(listener => {
			this.removeListener(topic, listener);
		});
	}

	removeListener(topic, callback) {
		if (typeof this._listeners[topic] === 'undefined') {
			return;
		}
		if (this._listeners[topic].indexOf(callback) === -1) {
			return;
		}
		this._listeners[topic].splice(this._listeners[topic].indexOf(callback), 1);
		if (this._listeners[topic].length === 0) {
			delete this._listeners[topic];
			delete this._receiveBuffer[topic];
		}
	}

}

module.exports = KafkaEmitter;
