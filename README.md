# In Memory Key-Value Store Addons

This project provides classes to instantiate an in-memory key-value store and to
interact with it. The store relies on Active Objects from the [ProActive
Programming](https://github.com/ow2-proactive/programming) library for remote
accessibility. Using ProActive Programming allows to interface with existing
protocols from [ProActive Workflows and
Scheduling](https://github.com/ow2-proactive/scheduling) (e.g. PAMR).

Please note this key-value store includes a publish/subscribe API that enables
users to listen for changes.

## Disclaimer

The code is not ready yet for use in production and should not be integrated for
now in a release of ProActive Workflows and Scheduling. The API was designed
based on the requirements for Orange CLIF. It could most probably be
generalized, naming enhanced, etc. 

Besides, the source code includes some TODO and FIXME comments that should be
fixed. Finally, lot of tests are missing.

# Installation

Run the following Gradle command to generate JAR file:

``` gradle clean install jar ```

It will generate a JAR file in
`build/libs/inmemory-keyvalue-store-addons-X.Y.Z.jar`

Copy the JAR file in the `addons` folder of your ProActive installation(s).

