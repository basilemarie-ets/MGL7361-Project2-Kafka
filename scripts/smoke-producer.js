const { TOPICS, createKafka, buildEvent, createLogger, publish } = require('../shared/kafka');

const logger = createLogger('smoke-producer');
const producer = createKafka('smoke-producer').producer();

(async () => {
  await producer.connect();

  const orderId = 'ord-smoke-' + Date.now();   // le même pour les 3 événements
  const customer = 'test@mail.com';

  await publish(
    producer,
    TOPICS.ORDERS,
    buildEvent('OrderCreated', orderId,
      { customer, items: [{ sku: 'PEN-1', qty: 1 }], total: 2.5, simulatePaymentFailure: false },
      'smoke-producer'),
    logger
  );

  // À toi : StockReserved sur TOPICS.INVENTORY  (payload : customer, total)

  await publish(
    producer,
    TOPICS.INVENTORY,
    buildEvent('StockReserved', orderId,
        {customer, total:2.5},
        'smoke-producer'),
    logger
    )
  

  // À toi : PaymentCompleted sur TOPICS.PAYMENTS (payload : customer, amount, transactionId)

  await publish(
    producer,
    TOPICS.PAYMENTS,
    buildEvent('PaymentCompleted', orderId,
        {customer, amount: 2.5, transactionId:'tx-smoke-1'},
        'smoke-producer'),
    logger
    )


  await producer.disconnect();
  logger.info('Terminé.');
})().catch((e) => { logger.error(e.message); process.exit(1); });