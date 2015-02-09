joinery
==========

<dl>
  <dt>joinery [joi-nuh-ree]</dt>
  <dd>
    1. In woodworking, the craft of joining together
    pieces of wood to produce more complex items.
  </dd>
  <dd>
    2. In Java, a data analysis library for joining together
    pieces of data to produce insight.
  </dd>
</dl>

quick start
==========

Remember **FizzBuzz** (of course you do!), well imagine you have just
solved the puzzle (well done!) and you have written the results to
a comma-delimited file for further analysis.  Now you want to know
how many times are the strings `"Fizz"`, `"Buzz"`, and `"FizzBuzz"`
printed out.

You could answer this question any number of ways, for example you
could modify the original program, or reach for Python/pandas, or even
(for the sadistic among us, you know who you are) type out a one-liner
at the command prompt (probably including `cut`, `sort`, and `uniq`).

Well, now you have one more option.  This option is especially good
if you are 1) using Java already and 2) may need to integrate your
solution with other Java applications in the future.

You can answer this question with **joinery**.

```java
df.groupBy("value")
  .count()
  .sortBy("-number")
  .head(3)
```

Printing out the resulting data frame gives us the following table.

```
    value   number
0   Fizz        27
1   Buzz        14
2   FizzBuzz    6
```

See [FizzBuzz.java](https://github.com/cardillo/joinery/blob/master/src/examples/java/FizzBuzz.java)
for the complete code.

maven
==========

A maven repository for **joinery** is hosted on
[JCenter](http://jcenter.bintray.com/).  For
instructions on setting up your maven profile to
use JCenter, visit https://bintray.com/bintray/jcenter.

```xml
<dependencies>
  ...
  <dependency>
    <groupId>joinery</groupId>
    <artifactId>joinery-dataframe</artifactId>
    <version>1.0</version>
  </dependency>
  ...
</dependencies>
```

download
==========

JCenter also allows for direct download using the button below.

[![Download](https://api.bintray.com/packages/cardillo/maven/joinery/images/download.svg)](https://bintray.com/cardillo/maven/joinery/_latestVersion)

documentation
==========

The complete api documentation for the `DataFrame` class is available
at http://cardillo.github.io/joinery

----------

[![Build Status](https://travis-ci.org/cardillo/joinery.svg?branch=master)](https://travis-ci.org/cardillo/joinery)
