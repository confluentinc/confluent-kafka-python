import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

class JavaConsumer {
  private Consumer instance;

  private JavaConsumer(String[] args) {
    Properties props = parseProperties(args);
    instance = new KafkaConsumer(props);
  }

  private static Properties parseProperties(String[] args) {
    /* Defaults */
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

    return JavaTestRunner.fromArgs(props, args);
  }

  private void subscribe(String topic) {
    instance.subscribe(Arrays.asList(topic.split(",")));
  }

  private void poll(String timeout) {
    ConsumerRecords<?, ?> records = instance.poll(Duration.ofSeconds(Integer.parseInt(timeout)));

    if (records.isEmpty()) {
      System.out.println("null");
    }

    records.forEach(record -> {
      System.out.printf("%s:%d:%d:%s:%s\n",
              record.topic(), record.partition(), record.offset(),
              record.key(), record.value());
    });
  }

  private void close() {
    instance.close();
  }

  static int run(String[] args) {
    if (args.length < 2)
      throw new IllegalArgumentException("At a minimum bootstrap.servers must be specified");

    JavaConsumer consumer = new JavaConsumer(args);
    Scanner console = new Scanner(new InputStreamReader(System.in, StandardCharsets.UTF_8));

    String cmd;
    String[] cmd_args;
    while (console.hasNext()) {
        cmd_args = console.nextLine().split(" ", 2);
        cmd = cmd_args[0];
      switch (cmd) {
          case "poll":
            consumer.poll(cmd_args[1]);
            break;
        case "subscribe":
          consumer.subscribe(cmd_args[1]);
          break;
        case "close":
          console.close();
          consumer.close();
          return 0;
        default:
          System.err.printf("Unrecognized command %s\n", cmd);
      }
    }
    return 0;
  }
}