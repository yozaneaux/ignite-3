* [Prerequisites](#prerequisites)
* [Building Ignite](#building-ignite)
* [Running sanity checks](#running-sanity-checks)
* [Running tests](#running-tests)
* [Checking and generating Javadoc](#checking-and-generating-javadoc)
* [Setting up IntelliJ Idea project](#setting-up-intellij-idea-project)
* [Code structure](#code-structure)
* [Release candidate verification](#release-candidate-verification)
***

## Prerequisites
 * Java 11 SDK
 * Maven 3.6.0+ (for building)
***


## Building Ignite
Ignite follows standard guidelines for multi-module maven projects, so it can be easily built using the following command from the project root directory (you can disable the tests when building using `-DskipTests`):
```
mvn clean package [-DskipTests]
```
Upon build completion, CLI tool can be found be under `modules/cli/target` directory. Use `ignite` on Linux and MacOS, or `ignite.exe` on Windows.
***


## Running sanity checks
### Code style
Code style is checked with [Apache Maven Checkstyle Plugin](https://maven.apache.org/plugins/maven-checkstyle-plugin/).
* [Checkstyle rules](check-rules/checkstyle-rules.xml)
* [Checkstyle suppressions](check-rules/checkstyle-suppressions.xml)
* [Checkstyle rules for javadocs](https://checkstyle.sourceforge.io/config_javadoc.html)

It is enabled by default and is bound to `compile` phase.

Build project without code style check:
```
mvn clean <compile|package|install|deploy> -Dcheckstyle.skip
```

Run code style checks only:
```
mvn clean validate -Pcheckstyle -Dmaven.all-checks.skip
```

Run javadoc style checks for public api only:
```
mvn clean checkstyle:checkstyle-aggregate -P javadoc-public-api
```
>ℹ `javadoc-public-api` profile is required for enabling checkstyle rules for public API javadoc.

Code style check results are generated at:
* `target/site/checkstyle-aggregate.html`
* `target/checkstyle.xml`

### Legacy API
The project is checked for legacy APIs with [Modernizer Maven Plugin](https://github.com/gaul/modernizer-maven-plugin/).
* [Modernizer rules](check-rules/modernizer-rules.xml)

Plugin is enabled by default and is bound to `test-compile` phase (due to requirement to run on already compiled classes)

Build project without legacy API check:
```
mvn clean <compile|package|install|deploy> -Dmodernizer.skip
```

Run legacy API checks only:
```
mvn clean test-compile -Pmodernizer -Dmaven.all-checks.skip
```
or
```
mvn clean test-compile -Dmaven.all-checks.skip && mvn modernizer:modernizer
```

### License headers
Project files license headers match with required template is checked with [Apache Rat Maven Plugin](https://creadur.apache.org/rat/apache-rat-plugin/).
```
mvn clean apache-rat:check -pl :apache-ignite
```
License headers check result is generated at `target/rat.txt`

### PMD
Static code analyzer is run with [Apache Maven PMD Plugin](https://maven.apache.org/plugins/maven-pmd-plugin/). Precompilation is required 'cause PMD shoud be run on compiled code.
* [PMD rules](check-rules/pmd-rules.xml)
```
mvn clean compile pmd:check
```
PMD check result (only if there are any violations) is generated at `target/pmd.xml`.
***

### Maven
Project is supplied with number of custom scripts for Maven sanity check.
To run checks, execute:
```
bash check-rules/maven-check-scripts/run.sh
```
from root of the project.

Linux, MacOS, WSL (Windows Subsystem on Linux) or alike environment is required.
`xpath` should be present in PATH
***


## Running tests
Run unit tests only:
```
mvn test
```
Run unit + integration tests:
```
mvn integration-test
```
Run integration tests only:
```
mvn integration-test -Dskip.surefire.tests
```
***


## Checking and generating Javadoc
Javadoc is generated and checked for correctness with [Maven Javadoc Plugin](https://maven.apache.org/plugins/maven-javadoc-plugin/).
(Javadoc style check is described above in [Code style](#code-style) section)

Check Javadoc is correct (precompilation is required for resolving internal dependencies):
```
mvn compile javadoc:javadoc 
```
Build Javadoc jars (found in `target` directory of module):
```
mvn package -P javadoc -Dmaven.test.skip
```
Build Javadoc site (found in `target/site/apidocs/index.html`):
```
mvn compile javadoc:aggregate -P javadoc
```
>ℹ `javadoc` profile is required for excluding internal classes
***


## Setting up IntelliJ Idea project
You can quickly import Ignite project to your IDE using the root `pom.xml` file. In IntelliJ, choose `Open Project` from the `Quick Start` box or choose `Open...` from the `File` menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:
 * Open the `File` menu and select `Project Structure...`
 * In the SDKs section, ensure that a 1.11 JDK is selected (create one if none exist)
 * In the `Project` section, make sure the project language level is set to 11.0 as Ignite makes use of several Java 11
 language features

Ignite uses machine code generation for some of it's modules. To generate necessary production code, build the project using maven (see [Building Ignite](#building-ignite)).

Configure Idea code style (for IntelliJ Idea >= 2019):
 * File -> Settings -> Editor -> Code Style -> Scheme -> gear (Show Scheme Actions) -> Import Scheme -> IntelliJ IDEA code style XML
 * Choose: ${igniteProject}/idea/intellij-java-google-style.xml
 * Import schema
 * Reboot IntelliJ Idea

***


## Code structure
High-level modules structure and detailed modules description can be found in the [modules readme](modules/README.md).
***


## Release candidate verification
1. Build the package (this will also run unit tests and the license headers check)
    ```
    mvn clean package
    ```
1. Go to the `modules/cli/target` directory which now contains the packaged CLI tool
    ```
    cd modules/cli/target
    ```
1. Run the tool without parameters (full list of available commands should appear)
    ```
    ./ignite
    ```
1. Run the initialization step
    ```
    ./ignite init --repo=<path to Maven staging repository>
    ```
1. Install an additional dependency (Guava is used as an example)
    ```
    ./ignite module add mvn:com.google.guava:guava:23.0
    ```
1. Verify that Guava has been installed
    ```
    ./ignite module list
    ```
1. Start a node
    ```
    ./ignite node start myFirstNode
    ```
1. Check that the new node is up and running
    ```
    ./ignite node list
    ```
1. Stop the node
    ```
    ./ignite node stop myFirstNode
    ```
