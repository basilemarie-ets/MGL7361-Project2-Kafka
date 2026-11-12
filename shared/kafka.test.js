const test = require('node:test');
const assert = require('node:assert');
const { TOPICS, createKafka, buildEvent, createLogger } = require('./kafka');

test('TOPICS contient les 3 noms du contrat', () => {
  assert.strictEqual(TOPICS.ORDERS, 'orders.created');
  assert.strictEqual(TOPICS.INVENTORY, 'inventory.events');
  assert.strictEqual(TOPICS.PAYMENTS, 'payments.events');
});

test('createKafka renvoie un client avec producer et consumer', () => {
  const kafka = createKafka('test');
  assert.strictEqual(typeof kafka.producer, 'function');
  assert.strictEqual(typeof kafka.consumer, 'function');
});

test('buildEvent renvoie les champs du contrat', () => {
  const e = buildEvent('OrderCreated', 'ord-1', { customer: 'a@b.c' }, 'test');
  assert.strictEqual(e.eventType, 'OrderCreated');
  assert.strictEqual(e.orderId, 'ord-1');
  assert.strictEqual(e.source, 'test');
  assert.deepStrictEqual(e.payload, { customer: 'a@b.c' });
  assert.ok(e.eventId);
  assert.ok(e.occurredAt);
});

test('buildEvent refuse un événement sans orderId', () => {
  assert.throws(() => buildEvent('OrderCreated'));
});

test('deux événements ont des eventId différents', () => {
  const a = buildEvent('X', 'o', {}, 's');
  const b = buildEvent('X', 'o', {}, 's');
  assert.notStrictEqual(a.eventId, b.eventId);
});

test('createLogger préfixe avec le nom du service et le niveau', (t) => {
  const log = t.mock.method(console, 'log', () => {});
  createLogger('mon-service').warn('attention');
  const line = log.mock.calls[0].arguments[0];
  assert.ok(line.includes('[mon-service]'));
  assert.ok(line.includes('WARN'));
  assert.ok(line.includes('attention'));
});