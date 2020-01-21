import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.function.Function;

public class JavaTestRunner {

  static Properties fromArgs(Properties props, String[] args) {

    try {
      for (String arg : args) {
        String[] prop = arg.split("=", 2);
        props.setProperty(prop[0], prop[1]);
      }
    } catch (Exception e) {
      System.err.printf("Prop %s missing value; skipping\n", Arrays.toString(args));
    }
    return props;
  }

  public static void main(String[] args) {
    final HashMap<String, Function<String[], Integer>> commands = new HashMap<>();

    commands.put("producer", JavaProducer::produce);
    commands.put("consumer", JavaConsumer::run);

    try {
      if (args.length < 1)
        throw new IllegalArgumentException("Exactly one command must be specified");

      if (!commands.containsKey(args[0])) {
        System.err.printf("Unrecognized command %s ", args[0]);
        System.exit(1);
      }

      commands.get(args[0]).apply(Arrays.copyOfRange(args, 1, args.length));

    } catch (Exception e) {
      System.err.printf("Unexpected error: %s\n", e);
      System.exit(1);
    }
    System.exit(0);
  }
}
