const { Kafka } = require('kafkajs')
var fs = require('fs');


const kafka = new Kafka({
clientId: 'my-app',
brokers: ['localhost:9092'],
})

log = "<h3>Nothing</h3>"

const run = async () => {
    const consumer = kafka.consumer({ groupId: 'represent' })

    await consumer.connect()
    await consumer.subscribe({ topic: 'represent', fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
          console.log(
            message.value,
          )
          log = "<h3>" + Date.now() + ": " + message.value + "</h3>"
          
      },
    })
}

run().catch(console.error)

const express = require("express");
const app = express();

app.listen(3000, () => {
  console.log("Application started and Listening on port 3000");
});



app.get("/", (req, res) => {
  res.setHeader("Content-Type", "text/html")
  res.send(log);
});

