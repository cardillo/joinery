package joinery;


import joinery.doctest.DocTestSuite;
import joinery.doctest.DocTestSuite.DocTestSourceDirectory;

import org.junit.runner.RunWith;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(DocTestSuite.class)
@DocTestSourceDirectory("src/main/java")
@SuiteClasses({DataFrame.class})
public class DocTest { }
