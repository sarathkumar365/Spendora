# Plaid Deferred Checklist (Future Step)

Plaid is intentionally deferred in Step 2. Use this checklist when enabling live integration.

## Credentials and Environment
- [ ] Add Plaid client id/secret configuration to secure local settings.
- [ ] Validate sandbox credential loading in API startup diagnostics.

## API Flow
- [ ] Implement `POST /api/v1/connections/plaid/link-token` with real Plaid SDK call.
- [ ] Implement `POST /api/v1/connections/plaid/exchange` token exchange and secure storage reference.
- [ ] Persist connection metadata in `connections` table.

## Sync Pipeline
- [ ] Implement accounts fetch and upsert.
- [ ] Implement cursor-based transaction sync.
- [ ] Normalize Plaid transactions into canonical transaction model.
- [ ] Preserve idempotency using `(account_id, external_txn_id)` uniqueness.

## Worker
- [ ] Add sync job type and worker execution path.
- [ ] Add retries/backoff for network failures.

## Verification
- [ ] Sandbox connect -> sync -> list transactions in UI.
- [ ] Repeated sync does not duplicate transactions.
- [ ] Error states are user-visible and actionable.
