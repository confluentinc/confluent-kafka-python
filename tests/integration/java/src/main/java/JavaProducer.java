import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

class JavaProducer {
  private final Producer instance;
  private final Function<String, ?> keyReader;
  private final Function<String, ?> valueReader;
  private final List<Future<RecordMetadata>> deliveryReports;

  private static final String fieldSeperator = "\t";

  private static Properties parseProperties(String[] args) {
    /* Defaults */
    Properties props = new Properties();
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    return JavaTestRunner.fromArgs(props, args);
  }

  private static Function<String, ?> configureReader(String serializer) {
    switch(serializer) {
      case "org.apache.kafka.common.serialization.DoubleSerializer":
        return Double::parseDouble;
      case "org.apache.kafka.common.serialization.FloatSerializer":
        return  Float::parseFloat;
      case "org.apache.kafka.common.serialization.ShortSerializer":
        return Short::parseShort;
      case "org.apache.kafka.common.serialization.LongSerializer":
        return Long::parseLong;
      case "org.apache.kafka.common.serialization.IntegerSerializer":
        return Integer::parseInt;
      case "org.apache.kafka.common.serialization.StringSerializer":
        return e -> e;
      default:
        return String::getBytes;
    }
  }

  private JavaProducer(String[] args) {
    Properties props = parseProperties(args);

    keyReader = configureReader(props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    valueReader = configureReader(props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

    deliveryReports = new LinkedList<>();

    instance = new KafkaProducer(props);
  }

  @SuppressWarnings("unchecked")
  private void send(String topic, String data) {
    String[] msg = data.split(":", 2);
    ProducerRecord record;
    if (msg.length == 2)
      record = new ProducerRecord(topic, keyReader.apply(msg[0]), valueReader.apply(msg[1]));
    else
      record = new ProducerRecord(topic, null, valueReader.apply(msg[0]));

    deliveryReports.add(instance.send(record));
  }

  private void flush() {
    instance.flush();

    while(!deliveryReports.isEmpty())
      poll();
  }

  private void poll() {
    List<Future<RecordMetadata>> reportable = deliveryReports.stream()
            .filter(Future::isDone)
            .collect(Collectors.toList());

    if (reportable.isEmpty()) {
      System.out.println("null null");
      return;
    }

    StringBuilder sb = new StringBuilder();
    for (Future<RecordMetadata> reportFuture : reportable) {
      sb.append(fieldSeperator);
      try {
        RecordMetadata report = reportFuture.get();
        sb.append(String.format("%s:%s:%d null",
                report.topic(), report.partition(), report.offset()));
      } catch (Exception e) {
        sb.append(String.format("null %s", e));
      }
    }
    deliveryReports.removeAll(reportable);
    System.out.println(sb.toString());
  }

  static int produce(String[] args) {
    if (args.length < 1)
      throw new IllegalArgumentException("At a minimum bootstrap.servers must be specified");

    JavaProducer producer = new JavaProducer(args);
    Scanner console = new Scanner(new InputStreamReader(System.in, StandardCharsets.UTF_8));

    String cmd;
    String[] cmd_args;
    while (console.hasNext()) {
      cmd_args = console.nextLine().split(" ", 3);

      cmd = cmd_args[0];
      switch (cmd) {
        case "send":
          producer.send(cmd_args[1], cmd_args[2]);
          break;
        case "flush":
          producer.flush();
          break;
        case "poll":
          producer.poll();
          break;
        default:
          System.err.printf("Unrecognized command %s\n", cmd);
      }
    }
    return 0;
  }
}

