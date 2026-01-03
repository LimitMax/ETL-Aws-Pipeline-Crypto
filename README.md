## How to Run Locally

### 1. Create virtual environment
```bash
python -m venv .venv
source .venv/bin/activate  # Linux / Mac
.venv\Scripts\activate     # Windows


##  2. Instal requirements
pip install -r requirements.txt

## 3. Run local ingestion pipeline (smoke test)
python main.py

## 4. Run automated tests
pytest -v
