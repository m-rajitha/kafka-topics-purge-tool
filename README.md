 Kafka topic data cleanup Java program

```markdown

# Kafka Topic Data Cleanup Java Program

This Java program is designed to help you clean up data from Kafka topics. It uses the Kafka AdminClient API to interact with Kafka clusters, describe topics, and delete records from topics based on certain conditions.

## Prerequisites

- Java JDK 20 or higher
- Kafka cluster access and permissions
- Maven (for building the project)

## Getting Started

1. Clone this repository:

   ```bash
   git clone https://github.com/m-rajitha/kafka-topics-purge-tool.git
   ```

2. Configure the cluster details on ClusterService class

3. Build the project:

   ```bash
   cd kafka-topic-cleanup
   mvn clean install
   ```

4. Run the program:

   ```bash
   java -jar target/kafka-topic-cleanup.jar
   ```

## Usage


1. Run the `Main` class to initiate the Kafka topic data cleanup process with args of cluster name , cluster environent and list of comma sepated topics.

2. The program will display a table showing the status of the cleanup process for each topic.

## Customization

You can customize the behavior of the program by modifying the code in the `TopicService` class. For example, you can change the cleanup conditions, error handling, or the way the cleanup status is displayed.

## Disclaimer

This program is provided as-is and without warranty. Use it at your own risk. Make sure to thoroughly test the program in a controlled environment before using it in production.

```
