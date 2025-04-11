# Heatwaves PySpark

A PySpark-based project for processing and analyzing heatwave data.

## 🔧 Setup Instructions

### 1. Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate 
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Jupyter Notebook

Ensure Docker is running. Then start the PySpark Jupyter environment with:

```bash
make start-jupyter
```

This will spin up a Docker container with Jupyter, exposing ports `8888` (Jupyter) and `4040` (Spark UI), and mount the current project directory for development.

### 4. Run tests

Make sure your virtual environment is activated, then:

```bash
pytest
```
