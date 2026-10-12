# Ansible deployment (single VM)

Deploys the whole stack (Kafka, Flink + jobs, Prink, Prometheus, Grafana) with
`docker compose up -d --build` on one Ubuntu 22.04 / 24.04 VM.

## What the playbook does

1. Installs Docker Engine + Compose v2 from the official Docker repository
   (skipped if `docker compose` is already available, e.g. on `*-Docker` cloud images).
2. Creates private data directories **outside** the repository checkout:
   `/data/stream-anon/input` and `/data/stream-anon/secrets` (mode `0700`).
3. Clones/updates the repository into `/opt/medical-data-stream-anonymization`
   at `app_version` (branch, tag or commit; default `main`).
4. Keeps Grafana's runtime state in `/var/lib/stream-anon/grafana` (seeded once
   from `grafana/` in the repo) via an Ansible-managed compose override, so the
   checkout stays clean and Grafana (uid 472) can write its database.
5. Builds and starts the stack, then waits until the Flink jobs are `RUNNING`.

When the deployed commit changes, the stack is stopped first (`compose_clean_restart: auto`)
so that no duplicate Flink jobs keep running in the old JobManager.

## Usage

```bash
cd deploy/ansible
ansible-galaxy collection install -r requirements.yml

# Create an inventory from the example (inventory/*.ini is gitignored) or keep
# it somewhere private, e.g. ../../secrets/
cp inventory/hosts.example.ini inventory/hosts.ini   # then fill in the host

ansible-playbook -i inventory/hosts.ini deploy.yml                     # main
ansible-playbook -i inventory/hosts.ini deploy.yml -e app_version=<branch|tag|sha>
```

Useful variables (`-e key=value`):

| Variable                      | Default                                  | Purpose |
|-------------------------------|------------------------------------------|---------|
| `app_version`                 | `main`                                   | Branch/tag/commit to deploy |
| `app_repo`                    | GitHub URL of this repo                  | Repository to deploy (e.g. a fork) |
| `data_dir`                    | `/data/stream-anon`                      | Private data root (`input/`, `secrets/`) |
| `compose_extra_files`         | `[]`                                     | Extra compose files (absolute paths), e.g. a private override in `/data/stream-anon/secrets` that mounts `/data/stream-anon/input` |
| `compose_clean_restart`       | `auto`                                   | `auto` / `always` / `never` stop the stack before `up` |
| `grafana_reset`               | `false`                                  | Re-seed Grafana state from the repo |
| `flink_expected_running_jobs` | `2`                                      | Jobs to wait for (`0` disables the check) |

## Accessing the UIs (SSH tunnel)

No service port is exposed publicly; only SSH is allowed. Forward the ports:

```bash
ssh -N -L 3000:localhost:3000 -L 8081:localhost:8081 -L 9090:localhost:9090 <vm>
# add -J <user>@<jumphost> if the VM is only reachable via a jump host
```

- Grafana: http://localhost:3000 (`admin` / `admin` on first login - change it)
- Flink UI: http://localhost:8081
- Prometheus: http://localhost:9090

Quick check on the VM:

```bash
curl -s localhost:8081/jobs/overview
sudo docker compose -f /opt/medical-data-stream-anonymization/docker-compose.yml \
  -f /etc/stream-anon/docker-compose.deploy.yml ps
```

## Private data

Never put real patient data, results computed on it, credentials or inventories
with real hosts into this repository. Use `/data/stream-anon/{input,secrets}` on
the VM and a private (gitignored) inventory on your machine.
