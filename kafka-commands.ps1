cd C:\kafka\kafka_2.13-3.6.0

# List all topics
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

# Describe a topic
.\bin\windows\kafka-topics.bat --describe --topic orders --bootstrap-server localhost:9092

# View messages in console
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic orders --from-beginning

# Check consumer group status
.\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group order-consumer-group

# Delete a topic (if needed)
.\bin\windows\kafka-topics.bat --delete --topic orders --bootstrap-server localhost:9092