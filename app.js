// Create a table element to display the data
const table = document.createElement('table');
document.body.appendChild(table);

// Create a header row for the table
const headerRow = table.insertRow();
headerRow.insertCell().innerText = 'Member Number';
headerRow.insertCell().innerText = 'Call Probability';
headerRow.insertCell().innerText = 'Raw Page Tags';
headerRow.insertCell().innerText = 'Intent';
headerRow.insertCell().innerText = 'P Value';
headerRow.insertCell().innerText = 'EVA DQ';

// Configure KafkaJS
const { Kafka } = require('kafkajs');
const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'my-app',
});

// Subscribe to the Kafka topic
const consumer = kafka.consumer({ groupId: 'my-group' });
async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });

  // Process each message received from Kafka
  await consumer.run({
    eachMessage: async ({ message }) => {
      // Parse the JSON data from the message
      const data = JSON.parse(message.value.toString());

      // Create a row for the data in the HTML table
      const row = table.insertRow();

      // Set the cell values for the row
      row.insertCell().innerText = data.memberNumber;
      row.insertCell().innerText = data.callProbability;
      row.insertCell().innerText = data.rawPageTags;
      row.insertCell().innerText = data.intent;
      row.insertCell().innerText = data.p_value;
      row.insertCell().innerText = data.EVA_DQ;

      // Keep the table size within a limit by removing the oldest row(s) if necessary
      const maxRows = 1000;
      if (table.rows.length > maxRows) {
        table.deleteRow(1);
      }
    },
  });
}

run().catch((error) => {
  console.error(error);
});

// Refresh the HTML table every 5 seconds
setInterval(() => {
  table.innerHTML = table.innerHTML;
}, 5000);
