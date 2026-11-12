const { TOPICS, createKafka, createLogger, runConsumer } = require('../shared/kafka');

const logger = createLogger('smoke-consumer');
const kafka = createKafka('smoke-consumer');

runConsumer({
  kafka,
  groupId: 'smoke-consumer-group',
  topics: Object.values(TOPICS),
  logger,
  handler: async (event) => logger.info(`Reçu : ${JSON.stringify(event)}`),
})
  .then(() => logger.info('En écoute sur les 3 topics...'))
  .catch((e) => { logger.error(e.message); process.exit(1); });