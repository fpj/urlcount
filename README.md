# Counting URLs

This is just a pretty elementary example of how to achieve exactly-once
semantics with an application that counts URLs. The functionality presented
here is clearly very limited, but this is just a proof of concept example
to show that for a class of applications, it is possible to have exactly-once
without requiring further changes to Apache Kafka.

## Running

This is a maven project and I'm currently using 0.9.0.0-SNAPSHOT, so you need to
either change the maven configuration or install manually the jar (which is what
I've done).

## Quick summary

The application essentially reads a log file containing URLs and uses the position
of the URL in the input stream to identify the log entry. That's a deterministic way
of identifying URL occurrences.

The URLs are published into a Kafka topic and consumed by a thread that adds the
received occurrences in a key-value store. The data store has a special feature, which
is to accept occurrences conditionally, based on the identifiers of the messages received.
More specifically, the consumer increments the occurrence of the URL only if the identifier
is greater than the one currently associated to the URL in the data store.