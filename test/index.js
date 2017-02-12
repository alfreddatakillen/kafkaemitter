const KafkaEmitter = require('../index');
const expect = require('chai').expect;
const td = require('testdouble');

describe('Unit:', () => {

	describe('KafkaEmitter instance', () => {

		it('has an .emit() function', () => {
			let kafka = new KafkaEmitter();
			expect(kafka.emit).to.be.a('function');
		});

		it('has an .on() function', () => {
			let kafka = new KafkaEmitter();
			expect(kafka.on).to.be.a('function');
		});

		describe('constructor()', () => {
			it('updates its configuration from the constructor argument', () => {
				let kafka = new KafkaEmitter({ requireAcks: 1337 });
				expect(kafka._config.requireAcks).to.deep.equal(1337);
			});
			
		});

		describe('_addIncomingMessage()', () => {
			it('should not add anything to topics that have no listeners', () => {
				let kafka = new KafkaEmitter();
				kafka.on('some-other-topic', msg => console.log(msg));
				let preBuffer = JSON.parse(JSON.stringify(kafka._receiveBuffer)); // Deep clone it.
				expect(Object.keys(preBuffer)).to.deep.equal([ 'some-other-topic' ]);
				kafka._addIncomingMessage('my-topic', JSON.stringify({ test: 'some-data' }));
				expect(kafka._receiveBuffer).to.deep.equal(preBuffer);
			});
			it('should not add anything if data is not JSON parsable', () => {
				let kafka = new KafkaEmitter();
				kafka.on('my-topic', msg => console.log(msg));
				let preBuffer = JSON.parse(JSON.stringify(kafka._receiveBuffer)); // Deep clone it.
				expect(Object.keys(preBuffer)).to.deep.equal([ 'my-topic' ]);
				let result = kafka._addIncomingMessage('my-topic', { test: 'some-data' });
				expect(kafka._receiveBuffer).to.deep.equal(preBuffer);
				expect(result).to.equal(false);
				result = kafka._addIncomingMessage('my-topic', 'sadđ{ßªðđ{ªßđð{ªđ{{');
				expect(kafka._receiveBuffer).to.deep.equal(preBuffer);
				expect(result).to.equal(false);
				result = kafka._addIncomingMessage('my-topic', [ 'whatever' ]);
				expect(kafka._receiveBuffer).to.deep.equal(preBuffer);
				expect(result).to.equal(false);
			});
			it('should return true if message was added', () => {
				let kafka = new KafkaEmitter();
				kafka.on('my-topic', msg => console.log(msg));
				let preBuffer = JSON.parse(JSON.stringify(kafka._receiveBuffer)); // Deep clone it.
				let result = kafka._addIncomingMessage('my-topic', JSON.stringify({ test: 'whatever' }));
				expect(kafka._receiveBuffer).to.not.deep.equal(preBuffer);
				expect(result).to.equal(true);
			});
		});

		describe('_initClient()', () => {
			it('always returns the same object', () => {
				let kafka = new KafkaEmitter();
				let clientObj0 = kafka._initClient();
				let clientObj1 = kafka._initClient();
				expect(clientObj0).to.equal(clientObj1);
			});
		});

		describe('_initConsumer()', () => {
			it('should always call the _initClient to get the client', () => {
				let kafka = new KafkaEmitter();
				td.replace(kafka, '_initClient');
				kafka._initConsumer();
				td.verify(kafka._initClient());
				td.reset();
			});
		});

		describe('_initProducer()', () => {
			it('should always call the _initProducer to get the client', () => {
				let kafka = new KafkaEmitter();
				td.replace(kafka, '_initClient');
				kafka._initProducer();
				td.verify(kafka._initClient());
				td.reset();
			});
		});

		describe('emit()', () => {
			it('adds new emits to an emit buffer', () => {
				let kafka = new KafkaEmitter();
				kafka.emit('test-topic', { test: 'some test' });
				expect(kafka._sendBuffer).to.deep.equal({
					'test-topic': [ JSON.stringify({ test: 'some test' }) ]
				});
				kafka.emit('test-topic', { test: 'another test' });
				expect(kafka._sendBuffer).to.deep.equal({
					'test-topic': [
						JSON.stringify({ test: 'some test' }),
						JSON.stringify({ test: 'another test' })
					]
				});
			});
			it('initializes a new kafka producer on first message', () => {
				let kafka = new KafkaEmitter();
				td.replace(kafka, '_initProducer');
				kafka.emit('test-topic', { test: 'some test' });
				td.verify(kafka._initProducer());
				td.reset();
			});
		});

		describe('eventNames()', () => {
			it('returns the topics we are listening at', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => console.log(msg);
				kafka.on('topic2', cb);
				kafka.on('topic0', cb);
				kafka.on('topic1', cb);
				expect(kafka.eventNames()).to.deep.equal([ // ...and the are sorted!
					'topic0',
					'topic1',
					'topic2'
				]);
			});
		});

		describe('listenerCount()', () => {
			it('returns nr of listeners', () => {
				let kafka = new KafkaEmitter();
				let cb0 = msg => console.log(msg);
				let cb1 = msg => console.log(msg);
				let cb2 = msg => console.log(msg);
				kafka.on('does-this-work', cb0);
				expect(kafka.listenerCount('does-this-work')).to.equal(1);
				kafka.on('does-this-work', cb1);
				expect(kafka.listenerCount('does-this-work')).to.equal(2);
				kafka.on('does-this-work', cb2);
				expect(kafka.listenerCount('does-this-work')).to.equal(3);
			});
		});

		describe('listeners()', () => {
			it('returns all listeners', () => {
				let kafka = new KafkaEmitter();
				let cb0 = msg => console.log(msg);
				let cb1 = msg => console.log(msg);
				let cb2 = msg => console.log(msg);
				kafka.on('does-this-work', cb0);
				kafka.on('does-this-work', cb1);
				kafka.on('does-this-work', cb2);
				expect(kafka.listenerCount('does-this-work')).to.equal(3);
				expect(kafka.listeners('does-this-work')[0]).to.equal(cb0);
				expect(kafka.listeners('does-this-work')[1]).to.equal(cb1);
				expect(kafka.listeners('does-this-work')[2]).to.equal(cb2);
			});
			it('does not return the same array as being used internally (for the sake of immutability)', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => console.log(msg);
				kafka.on('does-this-work', cb);
				expect(kafka.listeners('does-this-work')).to.not.equal(kafka._listeners['does-this-work']);
			});
		});

		describe('on()', () => {
			it('initializes a new kafka consumer on first listener', () => {
				let kafka = new KafkaEmitter();
				td.replace(kafka, '_initConsumer');
				kafka.on('test-topic', msg => console.log(msg));
				td.verify(kafka._initConsumer());
				td.reset();
			});
			it('creates a buffer when we start listening to a topic', () => {
				let kafka = new KafkaEmitter();
				kafka.on('this-is-a-test-topic', msg => console.log(msg));
				expect(kafka._receiveBuffer['this-is-a-test-topic']).to.be.an('array');
			});
			it('adds callback to the listeners array', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => console.log(msg);
				kafka.on('does-this-work', cb);
				expect(kafka.listeners('does-this-work')).to.include(cb);
			});
			it('does not allow the same listener added multiple times', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => console.log(msg);
				kafka.on('does-this-work', cb);
				kafka.on('does-this-work', cb);
				kafka.on('does-this-work', cb);
				expect(kafka.listeners('does-this-work').length).to.equal(1);
			});
		});

		describe('once()', () => {
			it('callbacks removes themselves from listener list when called', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => { let dummy = msg + 'test'; return dummy; };
				kafka.once('test-topic', cb);
				expect(kafka.listenerCount('test-topic')).to.equal(1);
				kafka._listeners['test-topic'][0](); // Call it!
				expect(kafka.listenerCount('test-topic')).to.equal(0);
			});
			it('should only be called once', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => { let dummy = msg + 'test'; return dummy; };
				kafka.once('test-topic', cb);
			});
		});

		describe('removeAllListeners()', () => {
			it('removes callbacks from the listeners array', () => {
				let kafka = new KafkaEmitter();
				let cb0 = msg => console.log(msg);
				let cb1 = msg => console.log(msg);
				kafka.on('does-this-work', cb0);
				kafka.on('does-this-work', cb1);
				expect(kafka.listenerCount('does-this-work')).to.equal(2);
				kafka.removeAllListeners('does-this-work');
				expect(kafka.listenerCount('does-this-work')).to.equal(0);
			});
			it('removes all listeners from all topics', () => {
				let kafka = new KafkaEmitter();
				let cb0 = msg => console.log(msg);
				let cb1 = msg => console.log(msg);
				let cb2 = msg => console.log(msg);
				let cb3 = msg => console.log(msg);
				kafka.on('does-this-work', cb0);
				kafka.on('does-this-work', cb1);
				kafka.on('my-topic', cb2);
				kafka.on('my-topic', cb3);
				expect(kafka.listenerCount('does-this-work')).to.equal(2);
				expect(kafka.listenerCount('my-topic')).to.equal(2);
				kafka.removeAllListeners();
				expect(kafka.listenerCount('does-this-work')).to.equal(0);
				expect(kafka.listenerCount('my-topic')).to.equal(0);
			});
		});

		describe('removeListener()', () => {
			it('removes callbacks from the listeners array', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => console.log(msg);
				kafka.on('does-this-work', cb);
				kafka.removeListener('does-this-work', cb);
				expect(kafka.listeners('does-this-work')).to.not.include(cb);
			});
			it('removes the topic from eventNames() if there are no listeners left', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => console.log(msg);
				kafka.on('does-this-work', cb);
				expect(kafka.eventNames()).to.deep.equal([ 'does-this-work' ]);
				kafka.removeListener('does-this-work', cb);
				expect(kafka.eventNames()).to.deep.equal([]);
			});
			it('removes the buffer for the topic', () => {
				let kafka = new KafkaEmitter();
				let cb = msg => console.log(msg);
				kafka.on('this-is-a-test-topic', cb);
				kafka.removeListener('this-is-a-test-topic', cb);
				expect(kafka._receiveBuffer['this-is-a-test-topic']).to.be.a('undefined');
			});
		});
	});
});
