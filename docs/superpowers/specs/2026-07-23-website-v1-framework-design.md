# Website V1 Reusable Framework Design

## Goal

Create a reusable first-version website framework on branch `website-v1`. The new
site will eventually replace the old Tencent Cloud site while keeping the same
cloud environment and domain. This task does not modify the production server.

## Chosen approach

Reuse the proven Python standard-library server and static HTML/CSS/JavaScript
dashboard from `web3project` instead of introducing a new frontend framework.
Keep the deployment portable with Docker and environment variables so the same
repository can run locally or on Tencent Cloud.

Alternatives considered:

- Copy the entire old repository: fastest, but mixes private research inputs,
  collection code, and deployment files.
- Rebuild with React and a separate API: more flexible, but adds unnecessary
  tooling and migration work for the first version.

## Boundaries

- `dashboard/` contains only the web server, public UI, and frontend dependencies.
- `data/public/` contains the curated files the public server may read.
- `scripts/` contains local run and public-snapshot preparation commands.
- `tests/` verifies payload generation, public/private separation, and health
  behavior.
- Collection and research pipeline code stays outside the public runtime.
- Private data, local state, API keys, and administrator controls are not copied.
- No Tencent Cloud production files are deleted or replaced in this task.

## Data flow

The existing `web3project` pipeline remains the source of the initial curated
price and volume research snapshot. A later refresh process may publish validated
files into `data/public/research/`. The website reads only that directory in
public mode and exposes JSON to the static browser application.

## Deployment and rollback

The Docker image contains `dashboard/` and `data/public/` only. Runtime settings
such as port and data path use environment variables. When production migration
is requested later, the old Tencent Cloud site must be backed up before the same
domain is switched to the new container.

## Validation

- Dashboard unit tests pass against the copied curated snapshot.
- The HTTP health endpoint returns success.
- The public server rejects private data paths and state writes.
- The Docker configuration exposes the runtime port without Render-specific
  configuration.

