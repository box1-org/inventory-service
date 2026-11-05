package org.box1;

import io.quarkus.test.junit.QuarkusIntegrationTest;

/** Repeat the HTTP contract against the packaged JVM application. */
@QuarkusIntegrationTest
class GreetingResourceIT extends GreetingResourceTest {}
