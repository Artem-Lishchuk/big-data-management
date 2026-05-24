# Project 4 — RICO Multimodal Pipeline Lab
## First-time setup

All commands below are run from the `project_4/` directory.

### 1. Start the infrastructure

```bash
make up
```

**Without Make (Windows / no `make` installed):**

```bash
docker compose up -d --wait postgres minio ollama
docker compose up -d minio-init ollama-init
docker compose up -d --wait airflow
```

### 2. Create and activate a Python virtual environment

```bash
python -m venv .venv-lab
```

**Linux / macOS:**

```bash
source .venv-lab/bin/activate
```

**Windows (PowerShell):**

```powershell
.venv-lab\Scripts\Activate.ps1
```

Your shell prompt should show `(.venv-lab)`.

### 3. Install Python dependencies

All Python packages are declared in `pyproject.toml`:

```bash
pip install --upgrade pip
pip install -e ".[dev]"
```

The install is large (~2–3 GB) because of PyTorch and the embedding models.

Airflow installs the same project automatically on first container start (`pip install -e /opt/project`).
Re-run `make airflow-install` after you change dependencies in `pyproject.toml`.

### 4. Open the lab notebook

```bash
jupyter lab notebook.ipynb
```

If Jupyter asks for a token, copy it from the terminal output or open the `http://localhost:8888/lab?token=…` URL it prints.

Run the notebook top to bottom. Section 0 pings each service — fix your stack before continuing if any health check fails.

---

## Day-to-day usage

**Start working** (containers were stopped):

```bash
make up
source .venv-lab/bin/activate   # or .venv-lab\Scripts\Activate.ps1 on Windows
jupyter lab notebook.ipynb
```

**Reset lab data** (replay the notebook on a clean slate — tables truncated, bucket cleared; models and volumes kept):

```bash
make reset
```

Then in Jupyter: **Kernel → Restart**, and run all cells again.

**Stop the stack** (data preserved in Docker volumes):

```bash
make down
```

**Full wipe** (delete all volumes — next `make up` re-runs DB migrations and re-pulls the Ollama model):

```bash
make clean
```

**Tail service logs:**

```bash
make logs
```