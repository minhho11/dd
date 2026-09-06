# Deploying `dd` to a VPS via GitHub Actions

The workflow [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml) builds a
static Linux binary, copies it to your VPS over SSH, and runs it with
`DATABASE_DSN` injected from a secret.

It is **manual** (`workflow_dispatch`) on purpose — `dd` generates traffic, so each
run should be deliberate.

---

## Step 1 — Prepare the VPS

SSH into the VPS and create the target directory:

```bash
sudo mkdir -p /opt/dd
sudo chown "$USER":"$USER" /opt/dd
```

(Optional) confirm outbound access to your Postgres from the VPS.

---

## Step 2 — Create an SSH deploy key

On your machine, generate a **dedicated** key pair for CI (no passphrase):

```bash
ssh-keygen -t ed25519 -f dd_deploy -C "github-actions-dd" -N ""
```

This makes `dd_deploy` (private) and `dd_deploy.pub` (public).

Add the **public** key to the VPS user that will run `dd`:

```bash
ssh-copy-id -i dd_deploy.pub <vps-user>@<vps-host>
# or append dd_deploy.pub to ~/.ssh/authorized_keys on the VPS manually
```

Test it:

```bash
ssh -i dd_deploy <vps-user>@<vps-host> "echo ok"
```

---

## Step 3 — Add GitHub repository secrets

Repo → **Settings → Secrets and variables → Actions → New repository secret**.
Create:

| Secret | Value | Example |
|---|---|---|
| `VPS_HOST` | VPS IP or hostname | `203.0.113.10` |
| `VPS_USER` | SSH user | `deploy` |
| `VPS_PORT` | SSH port | `22` |
| `VPS_SSH_KEY` | **contents of the private key** `dd_deploy` | `-----BEGIN OPENSSH PRIVATE KEY----- …` |
| `DATABASE_DSN` | Postgres DSN | `postgres://user:pass@db-host:5432/dd?sslmode=require` |

Paste the private key including the `BEGIN`/`END` lines:

```bash
cat dd_deploy   # copy the whole output into VPS_SSH_KEY
```

> Delete the local `dd_deploy` files afterwards, or store them somewhere safe.
> They are not needed again once the secret is set.

---

## Step 4 — Commit the workflow

The workflow file must be on your default branch for the "Run workflow" button to
appear:

```bash
git add .github/workflows/deploy.yml
git commit -m "ci: build + deploy to VPS"
git push origin main
```

---

## Step 5 — Run it

Repo → **Actions → build-and-deploy → Run workflow**. Fill in:

- **args** — the `dd` flags to run with, e.g.
  `-urls https://target.example -workers 50 -requests 1000 -cache-bust -human`
  (do **not** put `-dsn` here — `DATABASE_DSN` is injected automatically).
- **detached** — leave off to wait for the run and see the summary in the logs; turn
  on for long or "until blocked" runs so the job returns immediately.

The job will:

1. build `dd` for `linux/amd64`,
2. SCP it to `/opt/dd/dd` on the VPS,
3. run `DATABASE_DSN=… /opt/dd/dd <args>`.

For a **detached** run, output goes to `/opt/dd/dd.out` and errors to
`/opt/dd/err.log`:

```bash
ssh <vps-user>@<vps-host> "tail -f /opt/dd/dd.out"
```

---

## How the DSN is passed

`dd` reads `-dsn` from the `DATABASE_DSN` environment variable by default, so the
workflow just exports the secret before running — the DSN never appears in the
command line or the args input.

---

## Optional: run as a systemd service (scheduled / repeatable)

If you want the VPS to run `dd` on a schedule instead of on-demand from Actions,
keep the build+SCP steps and drop the "Run on VPS" step, then set up systemd once on
the VPS:

`/etc/dd/dd.env` (root-only: `sudo chmod 600 /etc/dd/dd.env`)

```ini
DATABASE_DSN=postgres://user:pass@db-host:5432/dd?sslmode=require
DD_ARGS=-urls https://target.example -workers 50 -requests 1000 -cache-bust
```

`/etc/systemd/system/dd.service`

```ini
[Unit]
Description=dd load run
After=network-online.target

[Service]
Type=oneshot
EnvironmentFile=/etc/dd/dd.env
ExecStart=/bin/sh -c '/opt/dd/dd $DD_ARGS'
User=deploy
```

`/etc/systemd/system/dd.timer` (e.g. hourly)

```ini
[Unit]
Description=Run dd hourly

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

Enable it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dd.timer
sudo systemctl start dd.service   # run once now
journalctl -u dd.service -f       # watch output
```

With this model the GitHub workflow only builds and uploads the new binary; systemd
runs it on the timer using the DSN from `/etc/dd/dd.env`.
