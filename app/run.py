"""
Databricks Apps startup script.

Adds the backend directory to sys.path and starts uvicorn,
ensuring all relative imports in main.py resolve correctly.
"""
import sys
import os

here = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(here, "backend")

if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

os.chdir(backend_dir)

import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
