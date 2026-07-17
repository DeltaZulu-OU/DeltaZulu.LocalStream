# ADR-0009: Naming Conventions — Topic, Subscription, and Processor Names

**Status:** Accepted

**Date:** 2026-07-17

## Context

Large distributed systems suffer from namespace collisions and naming confusion:
- Ambiguous topic names ("output", "data", "stream")
- Subscription names that don't indicate their purpose
- Cross-team coordination needed to avoid conflicts
- Historical baggage (renamed services with stale topic names)

DeltaZulu Platform has multiple consumers of stream data (archive, silver, logcluster) with clear ownership. Consistent naming conventions reduce friction and make the system self-documenting.

## Decision

**Use dot-separated, lowercase, domain-first naming for topics, subscriptions, and processors.**

### Topic Names

Format: `<domain>.<purpose>[.<qualifier>]`

**Rules:**

1. **Lowercase** — No uppercase or camelCase
2. **Dot-separated** — One dot minimum; hierarchical
3. **Domain first** — Logical owner (agent, silver, logcluster)
4. **Purpose second** — What data flows (output, events, status)
5. **Qualifier optional** — Sub-category (parser-status, network-events)
6. **Plural only when typed** — "logcluster.samples" (multiple samples), not "logcluster.samples-topic"
7. **No transport names** — Avoid "queue", "stream", "topic", "kafka" in the name
8. **No environment names** — No "prod", "staging", "dev" (handle via config)

**Good examples:**

```text
agent.output               # Primary agent output
agent.parser-status        # Parser state for each record
agent.deadletter           # Records that failed parsing
silver.process-events      # Normalized events for NRT processing
silver.network-sessions    # Derived sessions from network events
silver.authentications     # Derived auth from agent events
logcluster.samples         # Sampled/deduplicated records
logcluster.candidates      # Potential patterns for detection
```

**Bad examples:**

```text
KafkaAgentOutput           # ✗ Uppercase; names transport
agentOutputQueue           # ✗ camelCase; includes "queue"
prod-agent-output          # ✗ Includes environment
RelativePathQueue          # ✗ Uppercase; transport name
output                     # ✗ Too vague; no domain
```

### Subscription Names

Format: `<type>` or `<type>.<descriptor>` for processor subscriptions

**Patterns:**

1. **Simple name** — Logical consumer (archive, silver, logcluster)
2. **Processor prefix** — `processor.<processor-name>` for processor subscriptions

**Examples:**

```text
archive                    # Archive service subscriber
silver                     # Silver NRT pipeline subscriber
logcluster                 # LogCluster detection subscriber
debug-local                # Local developer debugging

processor.silver-normalizer        # Processor: normalizes to silver
processor.logcluster-sampler       # Processor: samples for logcluster
processor.parser-status-classifier # Processor: classifies parser errors
```

**Rules:**

1. **Lowercase with hyphens** — Separate words with hyphens
2. **No dots** (except processor prefix) — Subscriptions are not hierarchical
3. **Stable** — Do not rename subscriptions; create new ones if needed
4. **Meaningful** — Should indicate purpose (not `sub-1`, `consumer-abc`)
5. **Unique** — One subscription per subscriber/processor

### Processor Names

Format: `<action>-<component>` or `<component>-<action>`

**Patterns:**

- `silver-normalizer` — Processor that normalizes to silver
- `logcluster-sampler` — Processor that samples for logcluster
- `parser-status-classifier` — Processor that classifies parser status
- `dedup-by-source` — Processor that deduplicates by source

**Rules:**

1. **Lowercase with hyphens** — Consistent with subscription names
2. **Action-first or component-first** — Choose one pattern and stick to it (recommend action-first)
3. **Describe transformation** — What does the processor do?
4. **Unique per topic/output combination** — Same input but different output = different processor

## Consequences

### Positive

- ✓ Topics are self-documenting (domain + purpose obvious)
- ✓ Naming is predictable (no surprise "stream-output-v2")
- ✓ Namespacing prevents collisions (agent.X, silver.Y, logcluster.Z)
- ✓ Subscriptions are discoverable (grep for subscription names)
- ✓ Processor purpose is clear (silver-normalizer does what?)
- ✓ Consistency across teams (reduces negotiation)
- ✓ Stable; doesn't require renaming as system evolves

### Negative / Trade-offs

- ⚠ **Naming must be planned:** Cannot be ad-hoc
  - *Mitigation:* Create topics via configuration, not code; review new topic names
  
- ⚠ **Long names:** "silver.network-sessions" is longer than "network"
  - *Mitigation:* Length is acceptable; clarity > brevity
  
- ⚠ **Rigid conventions:** Hard to break rules for special cases
  - *Mitigation:* This is a feature; consistency is valuable; exceptions are rare

## Alternatives Considered

1. **Short codes (SV1, LG2, etc.):**
   - Pro: Brief
   - Con: Not self-documenting; must maintain mapping table
   - Decision: Rejected; opaque

2. **Kafka-style names (agent-output, agent-status):**
   - Pro: Simpler (fewer dots)
   - Con: Less hierarchical; namespacing relies on prefixes only
   - Decision: Rejected; dots provide clarity

3. **Include environment in topic name (prod-agent-output):**
   - Pro: Topic name indicates environment
   - Con: Cannot reuse topic names across environments; adds clutter
   - Decision: Rejected; environment should be in config, not name

4. **No naming convention; choose freely:**
   - Pro: Maximum flexibility
   - Con: Chaos; naming conflicts; hard to search
   - Decision: Rejected; convention is better

5. **Hierarchical subscription names (archive.agent, archive.silver):**
   - Pro: Structures subscriptions by relationship
   - Con: Overly complex for local system; most subscriptions are independent
   - Decision: Rejected; flat subscriptions are simpler

## Design Notes

### Why Dot-Separated?

Dots provide clear hierarchical structure:
- `agent.output` — clearly belongs to agent domain
- `silver.network-sessions` — clearly belongs to silver domain
- `logcluster.samples` — clearly belongs to logcluster domain

Hyphens are used within each level (e.g., `network-sessions`), not at the top level. This makes grepping easier:

```bash
grep -r "agent\." config/   # All agent-related topics
grep -r "silver\." config/  # All silver-related topics
```

### Why No Environment in Topic Name?

Topic names should be reusable across environments:

```yaml
# dev-config.yaml
localStream:
  topics:
    - name: agent.output
      retention: { maxAge: 1d }    # Keep less in dev

# prod-config.yaml
localStream:
  topics:
    - name: agent.output
      retention: { maxAge: 7d }    # Keep more in prod
```

Environment handling via configuration or directory structure, not naming.

### Evolving Topics

If a topic needs to evolve (schema change, new producer):

- **Approach 1 (rename):** Create new topic (e.g., `agent.output-v2`), migrate consumers
  - Pro: Explicit versioning
  - Con: Clutters namespace

- **Approach 2 (migrate in place):** Deprecate old topic, create new one with same name in new system
  - Pro: Single topic name
  - Con: Requires coordination

Recommend **Approach 1** (versioning) for LocalStream: evolution is rare and explicit versioning aids debugging.

## Related Decisions

- **ADR-0000:** Philosophy (self-documenting code)
- **ADR-0005:** DurableBuffer boundary (LocalStream topics, separate from DurableBuffer queues)

## Testing

- ✓ Topic names follow pattern `<domain>.<purpose>`
- ✓ Subscription names are stable (not renamed)
- ✓ Processor names indicate their transformation
- ✓ No namespace collisions across topics/subscriptions
- ✓ Configuration examples follow conventions

## Notes for Future Maintainers

**When adding a new topic:**
- Follow the naming pattern
- Update configuration examples
- Add to metrics / monitoring

**When adding a new processor:**
- Follow the naming pattern
- Create subscription with `processor.<name>` prefix
- Document what input is consumed and what output is produced

**When migrating/versioning:**
- Use suffix (e.g., `v2`, not `new-` or `-next`)
- Keep both versions until all consumers migrate
- Document migration path
