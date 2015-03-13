package joinery;

import joinery.js.JavascriptExpressionSuite;
import joinery.js.JavascriptExpressionSuite.JavascriptResource;

import org.junit.runner.RunWith;

@RunWith(JavascriptExpressionSuite.class)
@JavascriptResource(name="expressions.js")
public class DataFrameJavascriptMethodResolution {
    /* see test/resources/expressions.js for javascript expressions tested
     * this is stupid and hard to maintain,
     * but I'm tired of ambiguous method exceptions from js...
     */
}
