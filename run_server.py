#!/usr/bin/env python3
"""
Server entry point for News-dles web application.
"""

import sys
import os
from pathlib import Path

if __name__ == "__main__":
    # Add the src directory to the path
    src_path = Path(__file__).parent / "src"
    sys.path.insert(0, str(src_path))
    
    # Set environment variable for uvicorn to find our modules
    os.environ["PYTHONPATH"] = str(src_path)
    
    import uvicorn
    uvicorn.run(
        "crawleb.web.app:app",
        host="127.0.0.1", 
        port=8000,
        reload=True,
        log_level="info",
        reload_dirs=[str(src_path)]
    )