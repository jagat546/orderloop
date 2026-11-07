# Licensing

**Decided: Apache-2.0** (ADR 0008, `LICENSE`). The dependency audit below found nothing that conflicts with it. The options comparison is kept as the reasoning behind the choice. The employment-contract and IP check (roadmap P.1) must still pass before the repository goes public. This is not legal advice.

## Dependency audit

Audited 2026-11-07 over all nine reactor modules with `license-maven-plugin` 2.5.0, which reads each dependency's declared licence from its POM. Where a dependency is not plain Apache-2.0, the licence was cross-checked against the licence files inside its jar, where it has any, and against the jars actually packaged into the four service images (`BOOT-INF/lib` of each `-exec.jar`, 124 jars in all).

**Verdict: compatible.** Nothing that OrderLoop ships is under a strong copyleft licence (GPL or AGPL) without a permissive or weak-copyleft alternative. Every dual-licensed dependency offers a permissive or EPL-2.0 option, and OrderLoop takes that option rather than the LGPL or GPL one.

| Licence | Runtime artifacts | Verdict |
|---|---|---|
| Apache-2.0 | 116 | Compatible: the same licence |
| BSD-2-Clause, BSD-3-Clause, EDL-1.0 (a BSD-3-Clause licence), MIT | 16 | Compatible: permissive. Binary redistribution must keep their copyright notices |
| EPL-2.0, alone or as one option of a dual licence | 5 | Compatible when used unmodified as libraries (weak copyleft). Binary redistribution must say where their source is |
| GPL or AGPL with no alternative | 0 | None found |

The 137 runtime artifacts exclude the two OrderLoop modules. `spring-boot-jarmode-tools` 4.1.2 (Apache-2.0) is added to each image by the Spring Boot plugin and is not counted.

### Runtime dependencies not under Apache-2.0

| Dependency | Version | Upstream licence | Verdict |
|---|---|---|---|
| `ch.qos.logback:logback-core`, `logback-classic` | 1.5.38 | EPL-2.0 or LGPL-2.1-only | Compatible under EPL-2.0, used unmodified. The jars carry no licence file |
| `org.aspectj:aspectjweaver` | 1.9.25.1 | EPL-2.0 | Compatible, used unmodified. The jar carries no licence file |
| `jakarta.annotation:jakarta.annotation-api` | 3.0.0 | EPL-2.0 or GPL-2.0 with Classpath Exception | Compatible under EPL-2.0 |
| `jakarta.transaction:jakarta.transaction-api` | 2.0.1 | EPL-2.0 or GPL-2.0 with Classpath Exception | Compatible under EPL-2.0 |
| `jakarta.persistence:jakarta.persistence-api` | 3.2.0 | EDL-1.0 or EPL-2.0 | Compatible under EDL-1.0 |
| `jakarta.activation:jakarta.activation-api` | 2.1.4 | EDL-1.0 | Compatible |
| `jakarta.xml.bind:jakarta.xml.bind-api` | 4.0.5 | EDL-1.0 | Compatible |
| `org.glassfish.jaxb:jaxb-core`, `jaxb-runtime`, `txw2` | 4.0.9 | EDL-1.0 | Compatible |
| `com.sun.istack:istack-commons-runtime` | 4.1.2 | EDL-1.0 | Compatible |
| `org.eclipse.angus:angus-activation` | 2.0.3 | EDL-1.0 | Compatible |
| `org.postgresql:postgresql` | 42.7.14 | BSD-2-Clause | Compatible. It bundles the OnGres SCRAM and stringprep libraries, each with its own BSD licence file inside the jar |
| `com.google.protobuf:protobuf-java` | 4.36.2 | BSD-3-Clause | Compatible |
| `org.antlr:antlr4-runtime` | 4.13.2 | BSD-3-Clause | Compatible |
| `com.github.luben:zstd-jni` | 1.5.6-10 | BSD-2-Clause | Compatible |
| `org.hdrhistogram:HdrHistogram` | 2.2.2 | BSD-2-Clause or CC0 (public domain) | Compatible |
| `org.slf4j:slf4j-api`, `jul-to-slf4j` | 2.0.20 | MIT | Compatible |
| `org.checkerframework:checker-qual` | 3.55.1 | MIT | Compatible |

### Runtime dependencies under Apache-2.0

| Family | Version | Artifacts |
|---|---|---|
| Spring Boot (starters, autoconfiguration, actuator) | 4.1.2 | 43 |
| Spring Framework | 7.0.10 | 12 |
| Spring Security | 7.1.2 | 4 |
| Spring Data Commons, Spring Data JPA | 4.1.2 | 2 |
| Spring for Apache Kafka | 4.1.2 | 1 |
| Apache Kafka clients, with lz4-java 1.11.2 and snappy-java 1.1.10.7 | 4.2.2 | 3 |
| Apache Tomcat embedded (core, EL, WebSocket) | 11.0.27 | 3 |
| Hibernate ORM 7.4.13.Final, Hibernate Models 1.1.2, Hibernate Validator 9.1.5.Final, JBoss Logging 3.6.3.Final, ClassMate 1.7.3, Byte Buddy 1.18.15 | as listed | 6 |
| Jackson databind and core 3.1.7, Jackson annotations 2.21 | as listed | 3 |
| HikariCP | 7.0.2 | 1 |
| Flyway core and PostgreSQL support | 12.4.0 | 2 |
| Micrometer 1.17.2, Micrometer Tracing 1.7.2, context-propagation 1.2.1 | as listed | 8 |
| OpenTelemetry Java 1.62.0, OTLP protocol 1.10.0-alpha, semantic conventions 1.41.1 | as listed | 16 |
| OkHttp 5.3.2, Okio 3.16.4, Kotlin stdlib 2.3.21, JetBrains annotations 13.0 and 17.0.0 (the OTLP exporter's HTTP client) | as listed | 5 |
| Log4j API and Log4j-to-SLF4J 2.25.5, Commons Logging 1.3.6 | as listed | 3 |
| Jakarta Inject 2.0.1, Jakarta Validation 3.1.1, JSpecify 1.0.1, SnakeYAML 2.6 | as listed | 4 |

### Test-only dependencies

46 further artifacts are used only by tests and never packaged or distributed: JUnit 6.0.3 (EPL-2.0), Testcontainers 2.0.5 and Mockito 5.23.0 (MIT), Hamcrest 3.0 and ASM 9.7.1 (BSD-3-Clause), JNA 5.18.1 (Apache-2.0 or LGPL-2.1-or-later), ArchUnit 1.5.1, AssertJ 3.27.7, Awaitility 4.3.0, Spring Test, docker-java, Apache Commons and the JSON test helpers (Apache-2.0). Running them as test tools places no conditions on OrderLoop's own licence.

### Infrastructure images in Compose

The Compose files pull these images and run them unmodified as separate processes. OrderLoop neither links them nor redistributes them, so their licences do not affect OrderLoop's. Anyone who redistributes the images themselves, rather than pulling them, takes on their terms.

| Image | Upstream licence |
|---|---|
| `postgres` 18.6 | PostgreSQL Licence (permissive) |
| `apache/kafka` 4.4.0, `prom/prometheus` v3.15.0 | Apache-2.0 |
| `grafana/grafana` 13.2.4 | AGPL-3.0 (read from the image's `LICENSE`) |
| `grafana/tempo` 2.10.9 | AGPL-3.0, per upstream; not read from the image |
| `redis` 8.10.3 | RSALv2, SSPLv1 or AGPL-3.0, per upstream; not read from the image. No Java module uses Redis yet |
| `eclipse-temurin` 25 JRE (base of the service images) | GPL-2.0 with Classpath Exception |

### NOTICE file

**No `NOTICE` file is needed now.** Apache-2.0 section 4(d) applies to whoever redistributes a work that ships a `NOTICE` file. The repository contains no third-party code (no copied or vendored source), so it redistributes nothing that carries one, and a project `NOTICE` of OrderLoop's own is optional.

**Binary releases change this.** OrderLoop publishes no jars or images today: the demo builds its images locally (`pull_policy: never`). Before a release publishes the `-exec.jar` files or the service images:

- 73 of the 124 packaged jars carry their own `META-INF/NOTICE`. They ship unmodified inside `BOOT-INF/lib`, which keeps those notices with them.
- 38 jars carry no licence file at all, including Logback, AspectJ, `kafka-clients`, OpenTelemetry, `protobuf-java`, `antlr4-runtime` and `zstd-jni`.

So a release must add a third-party notice to each image and jar, listing every bundled dependency, its licence text and, for EPL-2.0, where its source is. The plugin's `add-third-party` output is the starting point.

### Re-running the audit

```bash
mvn -B package -DskipTests org.codehaus.mojo:license-maven-plugin:2.5.0:aggregate-add-third-party \
  -Dlicense.excludedScopes=test,system,provided
```

The plugin writes `target/generated-sources/license/THIRD-PARTY.txt`. Drop `test,` from the excluded scopes to include test dependencies. Re-run after any dependency upgrade, and before every release.

## The options compared

Both options are OSI-approved, and both can include the dependencies above.

### Apache-2.0

Permissive: anyone may use, modify, redistribute and sell it, including in closed-source products, if they keep the licence and notices and mark changed files.

| Gains | Costs |
|---|---|
| Widest adoption. Most companies allow Apache-2.0 dependencies without legal review | Anyone can run a closed, hosted fork and compete with your own hosted offering, giving nothing back |
| Explicit patent grant from contributors, ending if the licensee sues over patents | A later move to a stricter licence covers only new versions; released versions stay Apache-2.0 |
| Simple for contributors; the inbound licence can be the same as the outbound one | Little leverage for dual licensing, since anyone may already use it commercially |
| Code can move into GPLv3 or AGPLv3 projects (not the reverse) | |

### AGPL-3.0

Strong copyleft with a network clause: anyone who modifies it and lets users interact with it over a network must offer those users the modified source under AGPL-3.0.

| Gains | Costs |
|---|---|
| A hosted competitor must publish its changes, which protects a future hosted offering | Many companies ban AGPL software by policy, which reduces adoption and outside contributions |
| Allows dual licensing (AGPL for everyone, a commercial licence for those who want to avoid copyleft), if you hold or are licensed all copyright | Dual licensing needs a contributor licence agreement or copyright assignment, which deters some contributors |
| Includes a patent licence from contributors (section 11) | Operators must provide a way to get the source of any modified version they host, which is extra work for self-hosters who customise it |
| | Its effect on code that only talks to it over HTTP or Kafka is debated, so integrators may ask for legal review |

### Questions that decide it

1. Do you intend to sell a hosted version? If yes, AGPL-3.0 protects that; if no, Apache-2.0 maximises reach.
2. Do you want company contributors? Apache-2.0 makes that far easier.
3. Does your employment contract affect who owns this code? Answer that first.
