package examples;

import java.util.Arrays;
import java.util.List;

import joinery.DataFrame;

public class FizzBuzz {
    public static void main(final String[] args) {
        // generate data frame with numbers 1-100
        final DataFrame<Integer> input = new DataFrame<>("number");
        for (int i = 1; i <= 100; i++) {
            input.append(Arrays.asList(i));
        }

        // apply transform to "solve" fizz buzz
        final DataFrame<Object> df = input
                .add("value")
                .transform(new DataFrame.RowFunction<Integer, Object>() {
                    @Override
                    public List<List<Object>> apply(final List<Integer> row) {
                        final int value = row.get(0);
                        return Arrays.asList(
                            Arrays.<Object>asList(
                                value,
                                value % 15 == 0 ? "FizzBuzz" :
                                value %    3 == 0 ? "Fizz        " :
                                value %    5 == 0 ? "Buzz        " :
                                String.valueOf(value)
                            )
                        );
                    }
                });

        // group, count, sort, and display the top results
        System.out.println(
                df.groupBy("value")
                    .count()
                    .sortBy("-number")
                    .head(3)
                    .resetIndex()
            );
    }
}
