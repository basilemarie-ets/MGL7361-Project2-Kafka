const { Kafka, logLevel } = require('kafkajs');
const crypto = require('crypto');

const TOPICS = {ORDERS : "orders.created", INVENTORY : "inventory.events", PAYMENTS : "payments.events"}

function createKafka(clientId){
    const adresse = process.env.KAFKA_BROKERS || 'localhost:29092' //variable qui ne change qu'à un endroit
    const tabBroker = adresse.split(',')
    return new Kafka({
        clientId,
        brokers : tabBroker,
        logLevel: logLevel.WARN,
        retry: { initialRetryTime: 500, retries: 15 }
        }
    )
}

function buildEvent(eventType, orderId, payload, source){
    if(eventType == null || orderId == null){
        throw new Error("eventType or orderId is missing")
    }
    return {
        eventId : crypto.randomUUID(),
        eventType : eventType,
        orderId : orderId,
        payload : payload,
        source : source,
        occurredAt : new Date().toISOString()
    }
}

//log les info, warn et error
function createLogger(service){
    const log = (level,msg) => {console.log(`${new Date().toISOString()} [${service}] ${level} ${msg}`)}
    return ({
        info : (m) => {log('INFO', m)},
        warn : (m) => {log('WARN', m)},
        error : (m) => {log('ERROR', m)},
    })
}

async function publish(producer, topic, event, logger){
    const result = await producer.send({
        topic,
        messages : [{ key : event.orderId, value : JSON.stringify(event) }]
    })
    const meta = result[0];
    logger.info(`PUBLISH -> ${topic} [p${meta.partition}@${meta.baseOffset}] ${event.eventType} order=${event.orderId}`);
}

async function runConsumer({ kafka, groupId, topics, handler, logger }) {

    const consumer = kafka.consumer({groupId});
    await consumer.connect();

    for (const topic of topics){
        await consumer.subscribe({topic, fromBeginning:true});
    }

    await consumer.run({
        eachMessage : async ({topic, partition, message}) => {
            let event;
            try {
                event = JSON.parse(message.value.toString())
            } catch(err){
                return logger.error(`message illisible sur ${topic} [p${partition}@${message.offset}] : ${err.message}`);
            }

            logger.info(`CONSUME <- ${topic} [p${partition}@${message.offset}] ${event.eventType} order=${event.orderId}`)

            try {
                await handler(event, topic);
            }catch (err){
                logger.error(`échec du traitement de ${event.eventType} (${event.orderId}) : ${err.message}`);
            }
        }

        
    })
    return consumer

}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

module.exports = {TOPICS, createKafka, buildEvent, createLogger,publish, runConsumer, sleep };
