const Kafka = window.KafkaJS.Kafka;
const consumer = new Kafka.Consumer({
  groupId: 'my-group',
  brokers: ['your-broker-url']
});

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'my-topic', fromBeginning: true });

  // Process each message received from Kafka
  await consumer.run({
    eachMessage: async ({ message }) => {
      // Parse the JSON data from the message
      let data = message.value.toString();

      try {
        data = JSON.parse(data);
      } catch (error) {
        // Do nothing, assume data is already in the correct format
      }

      // Create a row for the data in the HTML table
      const table = document.getElementById('my-table');
      const row = table.insertRow();

      // Set the cell values for the row
      row.insertCell().innerText = data.memberNumber || data.member_number;
      row.insertCell().innerText = data.callProbability || data.call_probability;
      row.insertCell().innerText = data.rawPageTags || data.raw_page_tags;
      row.insertCell().innerText = data.intent;
      row.insertCell().innerText = data.p_value || data.pValue;
      row.insertCell().innerText = data.EVA_DQ || data.evaDq;

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
  const table = document.getElementById('my-table');
  table.innerHTML = table.innerHTML;
}, 5000);
