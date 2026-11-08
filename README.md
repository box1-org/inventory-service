# inventory-service

Owns per-site SKU stock, available-to-promise, reservations, expiry and denial reasons for Box1 operations. It consumes `order.placed.v1` and publishes exactly one outcome per order. It never updates order records.

Owner: `user:default/mike`. Quarkus 3.40.1.1, Java 21, Maven 3.9.16.

- **API contract:** `openapi/inventory-service.yaml` in [box1-ops-contracts](https://github.com/box1-org/box1-ops-contracts). Reads: `GET /api/v1/inventory?siteId=&sku=`, `GET /api/v1/reservations/{id}`. Operator routes under `/internal/v1` (restock, release, and create-once sites and stock records) need a verified token with the `inventory-admin` role.
- **Events:** consumes `box1.order.events.v1` and `box1.fulfillment.events.v1` as group `inventory`. Publishes to `box1.inventory.events.v1`, keyed by `orderId`. Dead letters go to `box1.inventory.dlq.v1`.
- **Lifecycle and reason codes:** `INVENTORY-RESERVATIONS.md` in the contracts repository.
- **Design and operations:** [docs/architecture.md](docs/architecture.md), [docs/runbook.md](docs/runbook.md).

**The service will not start without `INVENTORY_ORDER_EVENTS_FLOOR`.** See [First deployment](docs/runbook.md#first-deployment) and [the offset policy](docs/runbook.md#first-start-offsets) before deploying it anywhere that has retained events.

## Local development

Install JDK 21 and use the included Maven wrapper:

```sh
./mvnw quarkus:dev
./mvnw verify -DskipITs=false -Dgit.ref="$(git rev-parse HEAD)"
# With real PostgreSQL and Kafka, which CI does not have:
./mvnw clean verify -Pcontainers -DskipITs=false
curl http://localhost:8080/api/greetings?name=Ada
curl http://localhost:8080/api/info
```

Verification runs unit tests, HTTP tests, contract tests against the commit of
`box1-ops-contracts` pinned in `pom.xml`, packaged JVM integration tests,
Checkstyle, and SpotBugs/FindSecBugs. The failure scenarios are tagged `containers`,
because the CI task has no container runtime, so they run only with `-Pcontainers`.
With Podman, set `DOCKER_HOST` to its socket and `TESTCONTAINERS_RYUK_DISABLED=true`. `/api/info` includes the immutable Git SHA
embedded in the image; local builds default to `unknown` unless supplied above.
Java package `org.box1` is deliberately stable across generated services.

## First deployment

Create the service in Developer Hub. Its task tracks repository creation,
validated development onboarding, platform readiness, initial CI, image promotion,
and the verified first deployment. When the task completes, its output links to
source, catalog, initial pipeline, Argo CD, and the running service. No manual
onboarding merge or extra source commit is required.

If onboarding fails, the task reports the failed state and diagnostic link.
Submit the same name, owner, visibility, and description to resume the recorded
workflow. The repository and development resources are reused. A concurrent
request for the same service is rejected while the first task holds its lease.
The initial CI run is named `onb-inventory-service-<revision>`; a retried failed
build has a numbered suffix. Completed builds are reused.

Route: https://inventory-service-dev.apps.ocp.box1.org

## Delivery and configuration

PRs validate without registry credentials. Main releases use centrally managed
Tasks from `box1-gitops/platform/pipelines/tasks`, resolved from `box1-ci`.
GitHub checks link to cluster PipelineRuns named `inventory-service-validate-*`
and `inventory-service-release-*`. Pipelines as Code and Developer Hub use
separate GitHub Apps, both installed for all `box1-org` repositories.

Jib creates JVM images from a digest-pinned UBI 9 / Java 21 runtime without a
Docker daemon. Images use `registry.apps.ocp.box1.org/box1-ci/inventory-service:<SHA>`;
OpenShift pulls by digest through the internal registry service.

After onboarding, the GitOps repository is the authoritative deployment source.
Change the Route, probes, resources, or
`GREETING_PREFIX` there. The default smoke contract expects `Hello from box1`.
The ConfigMap also sets JVM container memory limits; runtime has no API token.

Endpoints: `/api/greetings`, `/api/info`, `/q/health/live`, `/q/health/ready`,
`/q/health/started`, `/q/openapi`, `/q/metrics`.

The development promotion policy is automated after checks. Other environments
need explicit platform configuration and approval. Static analysis is included;
no external dependency CVE database scan is implied.

## API documentation and endpoint testing

Developer Hub populates **Provides APIs** with `inventory-service-api`.
Open the API's **Definition** tab to browse the deployed OpenAPI contract and use
**Try it out**. The catalog reads `https://inventory-service-dev.apps.ocp.box1.org/q/openapi`,
so the API entity becomes available after the first successful deployment.
Swagger UI is also available at `https://inventory-service-dev.apps.ocp.box1.org/q/swagger-ui/`.
The production contract targets the public HTTPS Route; override `PUBLIC_API_URL`
when deploying behind another public address. CORS allows the Developer Hub
origin to make tester requests without cookies. API authentication, if added,
should be declared in the OpenAPI contract and supplied through **Authorize**.

## Documentation

Developer Hub publishes `docs/` on the service's **Docs** tab using TechDocs.
List new pages in `mkdocs.yml`.
