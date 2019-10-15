# Kafka sentence producer

A simple kafka producer written in Scala, that generates sentences using random english words

## How to Build it?

Just execute

```
sbt assembly
```

## How to use it?

The main expects 4 parameters:

1. host:port for your kafka installation. For example: `localhost:9092`
2. the topic you want to write messages to. For example: `test-topic`
3. the amount of sentences you want to generate. For example `1000`
4. the time to wait between each sentence, in milliseconds. For example `10`
