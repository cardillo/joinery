import java.util.Arrays;
import java.util.List;
import joinery.DataFrame;

public class FizzBuzz {
  public static void main(String[] args) {
    DataFrame<Integer> input = new DataFrame<>("number");
    for (int i = 1; i <= 100; i++) {
      input.append(Arrays.asList(i));
    }

    DataFrame<Object> df = input
        .add("value")
        .transform(new DataFrame.RowFunction<Integer, Object>() {
          public List<List<Object>> apply(List<Integer> row) {
            int value = row.get(0);
            return Arrays.asList(
              Arrays.asList(
                value,
                value % 15 == 0 ? "FizzBuzz" :
                value %  3 == 0 ? "Fizz    " :
                value %  5 == 0 ? "Buzz    " :
                String.valueOf(value)
              )
            );
          }
        });

    System.out.println(
        df.groupBy("value")
          .count()
          .sortBy("-number")
          .head(3)
          .resetIndex()
      );
  }
}
