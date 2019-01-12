package bank.balance

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.sun.xml.internal.ws.api.client.ServiceInterceptor.aggregate
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KGroupedStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import java.time.Instant
import java.util.Properties

fun main(args: Array<String>) {
    val config = Properties()

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-application")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    // Exactly once processing!!
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

    // json Serde
    val jsonSerializer = JsonSerializer()
    val jsonDeserializer = JsonDeserializer()
    val jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)


    val builder = KStreamBuilder()

    // create the initial json object for balances
    val initialBalance = JsonNodeFactory.instance.objectNode()
    initialBalance.put("count", 0)
    initialBalance.put("balance", 0)
    initialBalance.put("time", Instant.ofEpochMilli(0L).toString())

    val bankTransactions = builder.stream(Serdes.String(), jsonSerde, "bank-transactions")

    val bankBalance = bankTransactions
        .groupByKey(Serdes.String(), jsonSerde)
        .aggregate(
            { initialBalance },
            { key, transaction, balance -> newBalance(transaction, balance) },
            jsonSerde,
            "bank-balance-agg"
        )

    bankBalance.to(Serdes.String(), jsonSerde, "bank-balance-exactly-once")


    val streams = KafkaStreams(builder, config)
    streams.cleanUp()
    streams.start()

    // print the topology
    println(streams.toString())

    Runtime.getRuntime().addShutdownHook(Thread(Runnable { streams.close() }))

}

private fun newBalance(transaction: JsonNode, balance: JsonNode): JsonNode {
    // create a new balance json object
    val newBalance = JsonNodeFactory.instance.objectNode()
    newBalance.put("count", balance.get("count").asInt() + 1)
    newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt())

    val balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli()
    val transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli()
    val newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch))
    newBalance.put("time", newBalanceInstant.toString())
    return newBalance
}