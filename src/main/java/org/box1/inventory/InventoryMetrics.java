package org.box1.inventory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.box1.inventory.domain.ReasonCode;
import org.box1.inventory.store.InventoryStore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Business meters. Gauges read cached snapshots, so a scrape never runs SQL or a broker call. */
@ApplicationScoped
public class InventoryMetrics {
    private static final Logger LOG = Logger.getLogger(InventoryMetrics.class);
    private static final long ADMIN_TIMEOUT_SECONDS = 5;

    private final MeterRegistry registry;
    private final InventoryStore store;
    private final String bootstrapServers;
    private final String dlqTopic;
    private final ExpiryGate gate;
    private final AtomicReference<InventoryStore.Stats> stats =
            new AtomicReference<>(new InventoryStore.Stats(0, 0, 0, 0, 0, 0, 0));
    private final AtomicLong dlqDepth = new AtomicLong(-1);
    private final AtomicLong refreshFailures = new AtomicLong();
    private final AtomicLong dlqFailures = new AtomicLong();
    private Admin admin;

    public InventoryMetrics(MeterRegistry registry, InventoryStore store,
            @ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
            @ConfigProperty(name = "inventory.dlq.topic") String dlqTopic, ExpiryGate gate) {
        this.registry = registry;
        this.store = store;
        this.bootstrapServers = bootstrapServers;
        this.dlqTopic = dlqTopic;
        this.gate = gate;
        Gauge.builder("box1_outbox_pending", stats, s -> s.get().outboxPending()).register(registry);
        Gauge.builder("box1_outbox_oldest_pending_age_seconds", stats, s -> s.get().oldestPendingSeconds()).register(registry);
        Gauge.builder("box1_inventory_reservations_active", stats, s -> s.get().active()).register(registry);
        Gauge.builder("box1_inventory_reservations_committed", stats, s -> s.get().committed()).register(registry);
        Gauge.builder("box1_inventory_committed_oldest_age_seconds", stats, s -> s.get().oldestCommittedSeconds()).register(registry);
        Gauge.builder("box1_inventory_reconciliation_open", stats, s -> s.get().reconciliationOpen()).register(registry);
        Gauge.builder("box1_inventory_reserved_drift", stats, s -> s.get().reservedDrift()).register(registry);
        Gauge.builder("box1_inventory_dlq_depth", dlqDepth, AtomicLong::get).register(registry);
    }

    /** Refresh the database snapshot. On failure the last values stay and a failure counter rises. */
    @Scheduled(every = "${inventory.metrics.refresh-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void refresh() {
        try {
            stats.set(store.stats());
        } catch (RuntimeException error) {
            if (refreshFailures.getAndIncrement() == 0) {
                LOG.warn("Unable to refresh inventory gauges");
            }
            registry.counter("box1_inventory_metrics_refresh_failures", "source", "database").increment();
            return;
        }
        refreshFailures.set(0);
    }

    /** Refresh the dead-letter depth: end minus beginning offsets across the topic's partitions. */
    @Scheduled(every = "${inventory.metrics.refresh-interval}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void refreshDlqDepth() {
        try {
            dlqDepth.set(readDlqDepth());
            dlqFailures.set(0);
            gate.brokerReached();
        } catch (ExecutionException | TimeoutException | RuntimeException error) {
            if (dlqFailures.getAndIncrement() == 0) {
                LOG.warnv("Unable to read dead-letter depth: {0}", error.getClass().getSimpleName());
            }
            dlqDepth.set(-1);
            registry.counter("box1_inventory_metrics_refresh_failures", "source", "kafka").increment();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    private long readDlqDepth() throws InterruptedException, ExecutionException, TimeoutException {
        if (admin == null) {
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(AdminClientConfig.CLIENT_ID_CONFIG, "inventory-dlq-depth");
            config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 4000);
            config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 4000);
            admin = Admin.create(config);
        }
        List<TopicPartition> partitions = admin.describeTopics(List.of(dlqTopic)).allTopicNames()
                .get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(dlqTopic).partitions().stream()
                .map(p -> new TopicPartition(dlqTopic, p.partition())).toList();
        return offsets(partitions, OffsetSpec.latest()) - offsets(partitions, OffsetSpec.earliest());
    }

    private long offsets(List<TopicPartition> partitions, OffsetSpec spec)
            throws InterruptedException, ExecutionException, TimeoutException {
        Map<TopicPartition, OffsetSpec> request = partitions.stream().collect(Collectors.toMap(p -> p, p -> spec));
        return admin.listOffsets(request).all().get(ADMIN_TIMEOUT_SECONDS, TimeUnit.SECONDS).values().stream()
                .mapToLong(ListOffsetsResult.ListOffsetsResultInfo::offset).sum();
    }

    @PreDestroy
    void close() {
        if (admin != null) {
            admin.close(java.time.Duration.ofSeconds(2));
        }
    }

    /** Count units reserved at a site. Sites come from stock rows, so the tag stays bounded. */
    public void reserved(String siteId, int units) {
        registry.counter("box1_inventory_reserved_units", "site", siteId).increment(units);
    }

    /** Count a rejection by its closed reason set. */
    public void rejected(ReasonCode reason) {
        registry.counter("box1_inventory_rejections", "reason", reason.name()).increment();
    }

    /** Count a conflict that needed a retry. */
    public void conflictRetry() {
        registry.counter("box1_inventory_conflict_retries").increment();
    }

    /** Count any retry of an event after a transient failure. */
    public void retried() {
        registry.counter("box1_event_retries").increment();
    }

    /** Count an expired reservation. */
    public void expired() {
        registry.counter("box1_inventory_reservations_expired").increment();
    }

    /** Count a consumed event by type and outcome. Types are checked against a closed set before this runs. */
    public void consumed(String eventType, String outcome) {
        registry.counter("box1_events_consumed", "type", eventType, "outcome", outcome).increment();
    }

    /** Count a poll that left due holds alone, because events may be behind. */
    public void expiryHeld() {
        registry.counter("box1_inventory_expiry_held").increment();
    }

    /** Count a retry that used no attempt, because the database cannot serve. */
    public void heldForDatabase() {
        registry.counter("box1_inventory_consumer_held_for_database").increment();
    }

    /** Record how old an event was on arrival. */
    public void eventAge(double seconds) {
        registry.summary("box1_event_age_seconds").record(Math.max(seconds, 0));
    }

    /** Record the outbox lag of a confirmed publish and count it. */
    public void published(double lagSeconds) {
        registry.counter("box1_outbox_published").increment();
        registry.summary("box1_outbox_publish_lag_seconds").record(lagSeconds);
    }

    /** Count a publish attempt the broker did not confirm. */
    public void publishFailed() {
        registry.counter("box1_outbox_publish_failures").increment();
    }
}
