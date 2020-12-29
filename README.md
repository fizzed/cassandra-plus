Cassandra Plus by Fizzed
============================================

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.fizzed/cassandra-plus/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.fizzed/cassandra-plus)

[Fizzed, Inc.](http://fizzed.com) (Follow on Twitter: [@fizzed_inc](http://twitter.com/fizzed_inc))

## Overview

Utilities and framework integrations for Java and Cassandra. Includes an integration
of [Cassandra](https://cassandra.apache.org/) with the [Ninja Framework](https://github.com/ninjaframework/ninja).

## Ninja Framework

Ninja Framework module for Cassandra. Will help provide connectivity to Cassandra,
establish your sessions, and support for Cassandra database migrations.

### Setup

Add the cassandra-ninja-module dependency to your Maven pom.xml

```xml
<dependency>
    <groupId>com.fizzed</groupId>
    <artifactId>cassandra-ninja-module</artifactId>
    <version>1.0.0</version>
</dependency>
```

In your `conf/Module.java` file:

```java
package conf;

import com.fizzed.cassandra.ninja.NinjaCassandraModule;
import com.google.inject.AbstractModule;

public class Module extends AbstractModule {

    @Override
    protected void configure() {
        install(new NinjaCassandraModule());
    }

}
```

In your `conf/application.conf` file:

```java
#
# cassandra
#
cassandra.contact_points = localhost:19042
cassandra.keyspace = ninja_dev
cassandra.migrate.enabled = true
```

### Demo 

There is a Ninja app in the `demo` folder that demonstrates all the functionality
this module provides and it's a simple way to see how it works.  This project 
uses [Blaze](https://github.com/fizzed/blaze) to help script tasks. Run the
following in your shell (from the root project directory, not in `demo`):

    java -jar blaze.jar setup
    java -jar blaze.jar ninja

Once running, point your browser to http://localhost:18080/

## License

Copyright (C) 2020 Fizzed, Inc.

This work is licensed under the Apache License, Version 2.0. See LICENSE for details.