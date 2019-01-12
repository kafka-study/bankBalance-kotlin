package bank.balance

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.Properties
import java.util.concurrent.ThreadLocalRandom

fun main(args: Array<String>) {
    val properties = Properties()

    // kafka bootstrap server
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    // producer acks
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all") // strongest producing guarantee
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "1")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
    // leverage idempotent producer from Kafka 0.11 !
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true") // ensure we don't push duplicates

    val producer = KafkaProducer<String, String>(properties)

    var i = 0
    while (true) {
        println("Producing batch: $i")
        try {
            producer.send(newRandomTransaction("john"))
            Thread.sleep(100)
            producer.send(newRandomTransaction("stephane"))
            Thread.sleep(100)
            producer.send(newRandomTransaction("alice"))
            Thread.sleep(100)
            i += 1
        } catch (e: InterruptedException) {
            break
        }

    }
    producer.close()
}

fun newRandomTransaction(name: String): ProducerRecord<String, String> {
    // creates an empty json {}
    val transaction = JsonNodeFactory.instance.objectNode()

    // { "amount" : 46 } (46 is a random number between 0 and 100 excluded)
    val amount = ThreadLocalRandom.current().nextInt(0, 100)

    // Instant.now() is to get the current time using Java 8
    val now = Instant.now()

    // we write the data to the json document
    transaction.put("name", name)
    transaction.put("amount", amount)
    transaction.put("time", now.toString())
    return ProducerRecord("bank-transactions", name, transaction.toString())
}