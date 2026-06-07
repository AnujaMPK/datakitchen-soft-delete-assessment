# DK-812 – Deletion Preserving Load Mode

## Design

I introduced a new load mode called `soft_delete` that can be configured entirely through YAML:

```yaml
refresh:
  mode: soft_delete
```

The mode follows the same merge-based approach as `full_compare`, but instead of physically deleting records that disappear from the source, it preserves them using two metadata columns:

* `is_active`
* `deleted_at`

The configuration layer validates that:

* At least one primary key is defined.
* The required soft-delete columns exist.

This allows invalid configurations to fail early with a clear validation error before any data processing occurs.

## Behaviour

The load mode supports the following scenarios:

* Insert new records.
* Update existing records.
* Mark records inactive when they disappear from the source.
* Restore records when they reappear in the source.

When a record is restored, `is_active` is set back to `true` and `deleted_at` is cleared.

To preserve idempotency, only active rows are soft deleted. Rows that are already inactive are not updated again, preventing the deletion timestamp from changing on subsequent runs.

## Trade-offs

I chose to require `is_active` and `deleted_at` to be explicitly declared by model authors rather than injecting them automatically.

Advantages:

* Schema remains visible and explicit in model definitions.
* No hidden columns are added by the engine.
* Domain teams retain ownership of table structure.

Disadvantages:

* Additional configuration is required for adoption.
* Validation logic is slightly more complex.

I would revisit this if many teams adopted the feature and repetitive schema definitions became a maintenance burden.

## Risk

The biggest risk is assuming the incoming source dataset represents a complete snapshot.

If the source extract is incomplete or partially loaded, records may be incorrectly marked inactive.

To harden the implementation further, I would consider introducing snapshot completeness checks or additional operational safeguards before executing soft-delete logic.

## Follow-ups

Potential future improvements:

* Automatic schema validation for soft-delete metadata columns.
* Shared abstractions between `full_compare` and `soft_delete` to reduce duplicated merge logic.
* Additional integration tests covering larger datasets and edge cases.
* Metrics and monitoring for soft-delete and restore operations.

## Bonus – Deployment Workflow Fix

I identified a deployment issue in `deploy.yaml`.

The workflow comment states that deployment runs on merges to `main`, but the workflow was configured to run on `pull_request` events. I changed this to run on pushes to `main`.

Additionally, Terraform planning and applying were disconnected. The workflow generated and uploaded a reviewed `tfplan`, but the apply stage generated a fresh plan implicitly. This could allow production to diverge from the reviewed plan.

I updated the workflow so the apply stage downloads and applies the previously generated `tfplan`, ensuring the approved plan is the exact plan that gets deployed.
