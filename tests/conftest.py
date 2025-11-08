import os
from dotenv import load_dotenv

# Force load .env from project root even in dev mode
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENV_PATH = os.path.join(ROOT_DIR, ".env")

if os.path.exists(ENV_PATH):
    print(f"[pytest-init] Loading .env from: {ENV_PATH}")
    load_dotenv(ENV_PATH, override=True)
else:
    print(f"[pytest-init] WARNING: .env not found at {ENV_PATH}")
