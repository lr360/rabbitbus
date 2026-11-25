// [AI] amqplib calls the consumer with null when the broker cancels it (consumer cancel notification). The queue must
// report it on the bus as an error, so the bus owner reconnects, and must not hand it to the subscriber.
var EventEmitter = require('events').EventEmitter
var Queue = require('rabbitmq/queue')
var PubSubQueue = require('rabbitmq/pubsubqueue')

function flush() {
  return new Promise(function (resolve) {
    setImmediate(resolve)
  })
}

function createChannel() {
  return {
    assertExchange: jest.fn().mockResolvedValue({}),
    assertQueue: jest.fn().mockResolvedValue({}),
    bindQueue: jest.fn().mockResolvedValue({}),
    consume: jest.fn().mockResolvedValue({ consumerTag: 'tag' })
  }
}

function createBus() {
  var bus = new EventEmitter()
  bus.handleIncoming = jest.fn()
  return bus
}

describe('consumer cancelled by the broker', function () {
  it('queue: emits an error on the bus and does not call the listener', async function () {
    var channel = createChannel()
    var bus = createBus()
    var errors = []
    bus.on('error', function (err) { errors.push(err) })
    var callback = jest.fn()
    var queue = new Queue({ bus: bus, listenChannel: channel, sendChannel: channel, queueName: 'my.queue', log: function () {}, assertQueue: false })

    queue.listen(callback, { formatter: { deserialize: JSON.parse } })
    var consumer = channel.consume.mock.calls[0][1]
    consumer(null)

    expect(errors.length).toBe(1)
    expect(errors[0].message).toBe('Consumer cancelled by the broker on queue my.queue')
    expect(bus.handleIncoming).not.toHaveBeenCalled()
    expect(callback).not.toHaveBeenCalled()
  })

  it('pubsubqueue: emits an error on the bus and does not call the subscriber', async function () {
    var channel = createChannel()
    var bus = createBus()
    var errors = []
    bus.on('error', function (err) { errors.push(err) })
    var callback = jest.fn()
    var correlator = { queueName: function (options, cb) { cb(null, 'test-my.queue') } }
    var queue = new PubSubQueue({ bus: bus, correlator: correlator, listenChannel: channel, sendChannel: channel, queueName: 'my.queue', routingKey: 'my.event', log: function () {}, ack: true })

    queue.subscribe({ formatter: { deserialize: JSON.parse } }, callback)
    await flush()
    var consumer = channel.consume.mock.calls[0][1]
    consumer(null)

    expect(channel.consume.mock.calls[0][0]).toBe('test-my.queue')
    expect(errors.length).toBe(1)
    expect(errors[0].message).toBe('Consumer cancelled by the broker on queue test-my.queue')
    expect(bus.handleIncoming).not.toHaveBeenCalled()
    expect(callback).not.toHaveBeenCalled()
  })

  it('emits a single error per bus when several of its queues are cancelled', async function () {
    var channel = createChannel()
    var bus = createBus()
    var errors = []
    bus.on('error', function (err) { errors.push(err) })
    var correlator = { queueName: function (options, cb) { cb(null, 'test-' + options.queueName) } }
    var first = new PubSubQueue({ bus: bus, correlator: correlator, listenChannel: channel, sendChannel: channel, queueName: 'first', log: function () {} })
    var second = new Queue({ bus: bus, listenChannel: channel, sendChannel: channel, queueName: 'second', log: function () {}, assertQueue: false })

    first.subscribe({ queueName: 'first', formatter: { deserialize: JSON.parse } }, jest.fn())
    second.listen(jest.fn(), { formatter: { deserialize: JSON.parse } })
    await flush()
    var consumerOf = function (queueName) {
      return channel.consume.mock.calls.find(function (call) { return call[0] === queueName })[1]
    }
    consumerOf('test-first')(null)
    consumerOf('second')(null)

    expect(errors.length).toBe(1)
    expect(errors[0].message).toBe('Consumer cancelled by the broker on queue test-first')
  })

  it('pubsubqueue: still hands a delivered message to the bus', async function () {
    var channel = createChannel()
    var bus = createBus()
    var correlator = { queueName: function (options, cb) { cb(null, 'test-my.queue') } }
    var queue = new PubSubQueue({ bus: bus, correlator: correlator, listenChannel: channel, sendChannel: channel, queueName: 'my.queue', log: function () {} })

    queue.subscribe({ formatter: { deserialize: JSON.parse } }, jest.fn())
    await flush()
    channel.consume.mock.calls[0][1]({ content: Buffer.from('{"a":1}') })

    expect(bus.handleIncoming).toHaveBeenCalledTimes(1)
    expect(bus.handleIncoming.mock.calls[0][1].content).toEqual({ a: 1 })
  })
})

// [AI] When the broker refuses a declaration it closes the channel and every operation still pending on it is rejected:
// those rejections must be handled (the channel 'error' event already reports the failure), or the process crashes.
describe('setup refused by the broker', function () {
  var unhandled
  function onUnhandled(reason) { unhandled.push(reason) }

  beforeEach(function () {
    unhandled = []
    process.on('unhandledRejection', onUnhandled)
  })

  afterEach(function () {
    process.removeListener('unhandledRejection', onUnhandled)
  })

  it('pubsubqueue: logs a refused queue declaration and the operations rejected after it, without unhandled rejections', async function () {
    var channel = createChannel()
    channel.assertExchange.mockRejectedValue(new Error('Channel ended, no reply will be forthcoming'))
    channel.assertQueue.mockRejectedValue(new Error('Operation failed: QueueDeclare; 404 (NOT-FOUND)'))
    var log = jest.fn()
    var correlator = { queueName: function (options, cb) { cb(null, 'test-my.queue') } }
    var queue = new PubSubQueue({ bus: createBus(), correlator: correlator, listenChannel: channel, sendChannel: channel, confirmChannel: channel, queueName: 'my.queue', log: log, ack: true })

    queue.subscribe({ formatter: { deserialize: JSON.parse } }, jest.fn())
    await flush()
    await flush()

    expect(unhandled).toEqual([])
    expect(channel.consume).not.toHaveBeenCalled()
    expect(log).toHaveBeenCalledWith('%s failed: %s', 'subscribing to queue test-my.queue', 'Operation failed: QueueDeclare; 404 (NOT-FOUND)')
    expect(log).toHaveBeenCalledWith('%s failed: %s', 'asserting exchange amq.topic', 'Channel ended, no reply will be forthcoming')
  })

  it('pubsubqueue: logs a refused error queue declaration', async function () {
    var channel = createChannel()
    channel.assertQueue.mockResolvedValueOnce({}).mockRejectedValueOnce(new Error('Channel ended, no reply will be forthcoming'))
    var log = jest.fn()
    var correlator = { queueName: function (options, cb) { cb(null, 'test-my.queue') } }
    var queue = new PubSubQueue({ bus: createBus(), correlator: correlator, listenChannel: channel, sendChannel: channel, queueName: 'my.queue', log: log, ack: true })

    queue.subscribe({ formatter: { deserialize: JSON.parse } }, jest.fn())
    await flush()
    await flush()

    expect(unhandled).toEqual([])
    expect(channel.consume).not.toHaveBeenCalled()
    expect(log).toHaveBeenCalledWith('%s failed: %s', 'subscribing to queue test-my.queue', 'Channel ended, no reply will be forthcoming')
  })

  it('pubsubqueue and queue: log a refused consume', async function () {
    var channel = createChannel()
    channel.consume.mockRejectedValue(new Error('Channel ended, no reply will be forthcoming'))
    var log = jest.fn()
    var correlator = { queueName: function (options, cb) { cb(null, 'test-my.queue') } }
    var pubsub = new PubSubQueue({ bus: createBus(), correlator: correlator, listenChannel: channel, sendChannel: channel, queueName: 'my.queue', log: log })
    var queue = new Queue({ bus: createBus(), listenChannel: channel, sendChannel: channel, queueName: 'other.queue', log: log, assertQueue: false })

    pubsub.subscribe({ formatter: { deserialize: JSON.parse } }, jest.fn())
    queue.listen(jest.fn(), { formatter: { deserialize: JSON.parse } })
    await flush()
    await flush()

    expect(unhandled).toEqual([])
    expect(log).toHaveBeenCalledWith('%s failed: %s', 'consuming queue test-my.queue', 'Channel ended, no reply will be forthcoming')
    expect(log).toHaveBeenCalledWith('%s failed: %s', 'consuming queue other.queue', 'Channel ended, no reply will be forthcoming')
  })

  it('queue: does not emit an error without a listener when the error queue declaration is refused', async function () {
    var channel = createChannel()
    channel.assertQueue.mockResolvedValueOnce({}).mockRejectedValueOnce(new Error('Channel ended, no reply will be forthcoming'))
    var log = jest.fn()
    var queue = new Queue({ bus: createBus(), listenChannel: channel, sendChannel: channel, queueName: 'my.queue', log: log, ack: true })
    await flush()
    await flush()

    expect(unhandled).toEqual([])
    expect(queue.initialized).toBe(false)
    expect(log).toHaveBeenCalledWith('error connecting to queue %s. error: %s', 'my.queue', 'Error: Channel ended, no reply will be forthcoming')
  })

  it('queue: still emits the error to its listeners', async function () {
    var channel = createChannel()
    channel.assertQueue.mockRejectedValue(new Error('Operation failed: QueueDeclare; 404 (NOT-FOUND)'))
    var queue = new Queue({ bus: createBus(), listenChannel: channel, sendChannel: channel, queueName: 'my.queue', log: jest.fn() })
    var errors = []
    queue.on('error', function (err) { errors.push(err) })
    await flush()

    expect(errors.length).toBe(1)
    expect(unhandled).toEqual([])
  })
})
