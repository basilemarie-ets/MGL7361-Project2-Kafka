# MGL7361-Project2-Kafka
Concevoir et développer une preuve de concept d'une application distribuée basée sur une architecture orientée événements utilisant Apache Kafka. 

# MGL7361 – Projet 2 : architecture orientée événements (Kafka)

<!-- À toi : 2 phrases. Quel est le projet, et quelle est l'idée du flux
     (commande -> stock -> paiement -> notification) ? -->

## Prérequis
- Docker Desktop **démarré** (« Engine running »)
- Node.js 20 ou plus
- Git

## Démarrer Kafka
```bash
docker compose up -d
docker compose logs kafka-init
```
`kafka-init` doit lister `orders.created`, `inventory.events` et `payments.events`.
Kafka UI : http://localhost:8080

## Vérifier son installation
```bash
npm install
npm test
```
Puis, dans deux terminaux :
```bash
npm run smoke:consumer     # terminal 1, reste en écoute
npm run smoke:producer     # terminal 2
```
<!-- À toi : qu'est-ce qu'on doit voir si tout va bien ? (indice : tes 4 critères) -->

## Contrat des événements
<!-- À toi : un tableau avec les 3 topics et qui publie dedans, puis la liste des
     5 eventType avec leurs champs de payload. Reprends-le du document d'équipe. -->

## Utiliser `shared/kafka.js`
```js
const { TOPICS, createKafka, buildEvent, createLogger, publish, runConsumer } = require('../shared/kafka');

const logger = createLogger('mon-service');
const kafka = createKafka('mon-service');
const producer = kafka.producer();
await producer.connect();

// Publier (la clé orderId est gérée par publish)
await publish(
  producer,
  TOPICS.ORDERS,
  buildEvent('OrderCreated', orderId, { customer, items, total }, 'mon-service'),
  logger
);

// Consommer
await runConsumer({
  kafka,
  logger,
  groupId: 'mon-service-group',
  topics: [TOPICS.INVENTORY],
  handler: async (event) => {
    // réagir selon event.eventType
  },
});
```

## Règles d'équipe
1. Un topic par domaine : un seul service écrit dans un topic.
2. Toujours passer par `createKafka` et `TOPICS` : jamais d'adresse ni de nom de topic en dur.
3. Un `groupId` différent par service.
4. Un dossier = un propriétaire. Une branche par personne, pull request relue avant fusion dans `main`.
5. Ne pas modifier `shared/` ni le contrat sans prévenir l'équipe.

## Lancer un service en local
<!-- À toi : comment lance-t-on `node order-service/index.js` ?
     Précise ce qui se passe si on ne définit pas KAFKA_BROKERS, et pourquoi. -->

## Commandes utiles
```bash
docker compose ps -a        # état des conteneurs
docker compose logs -f kafka
docker compose down         # arrête
docker compose down -v      # arrête et efface les données
```