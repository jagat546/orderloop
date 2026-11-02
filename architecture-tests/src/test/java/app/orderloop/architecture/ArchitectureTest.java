package app.orderloop.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.library.GeneralCodingRules;

/** Invariants from CLAUDE.md that can be checked on bytecode, across every module. */
@AnalyzeClasses(packages = "app.orderloop", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  private static final String ASYNC = "org.springframework.scheduling.annotation.Async";

  @ArchTest
  static final ArchRule downstreamServicesNeverDependOnEachOther =
      slices()
          .matching("app.orderloop.(inventory|payment|fulfilment)..")
          .should()
          .notDependOnEachOther()
          .because("only the orchestrator decides what happens next");

  @ArchTest
  static final ArchRule tenantContextIsNotThreadLocal =
      noClasses()
          .should()
          .dependOnClassesThat()
          .areAssignableTo(ThreadLocal.class)
          .because("tenant context lives in ScopedValue (invariant 3)");

  @ArchTest
  static final ArchRule noAsyncClasses =
      noClasses()
          .should()
          .beAnnotatedWith(ASYNC)
          .because("scoped values do not cross into @Async threads (invariant 3)");

  @ArchTest
  static final ArchRule noAsyncMethods =
      noMethods()
          .should()
          .beAnnotatedWith(ASYNC)
          .because("scoped values do not cross into @Async threads (invariant 3)");

  @ArchTest
  static final ArchRule onlyTheOutboxRelayUsesKafkaTemplate =
      noClasses()
          .that()
          .resideOutsideOfPackage("..outbox..")
          .should()
          .dependOnClassesThat()
          .haveFullyQualifiedName("org.springframework.kafka.core.KafkaTemplate")
          .because("only the outbox relay publishes (invariant 4)");

  // GeneralCodingRules' java.util.logging rule only catches logger fields, not direct calls.
  @ArchTest
  static final ArchRule logThroughSlf4j =
      noClasses()
          .should()
          .dependOnClassesThat()
          .resideInAPackage("java.util.logging..")
          .because("logs go through SLF4J with tenant and trace IDs in MDC");

  @ArchTest
  static final ArchRule noStandardStreams =
      GeneralCodingRules.NO_CLASSES_SHOULD_ACCESS_STANDARD_STREAMS;
}
