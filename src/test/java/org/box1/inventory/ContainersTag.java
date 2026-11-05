package org.box1.inventory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.quarkus.test.common.QuarkusTestResource;
import org.junit.jupiter.api.Tag;

/** Runs against real PostgreSQL and Kafka containers. Requires -Pcontainers. */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Tag("containers")
@QuarkusTestResource(value = FullStackResource.class, restrictToAnnotatedClass = false)
@interface ContainersTag {}
