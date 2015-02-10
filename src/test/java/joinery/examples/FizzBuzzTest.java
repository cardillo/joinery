package joinery.examples;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import examples.FizzBuzz;

public class FizzBuzzTest {
    @Test
    public void testMain() {
        FizzBuzz.main(new String[] { });
        assertTrue("FizzBuzz example completes successfully", true);
    }
}
