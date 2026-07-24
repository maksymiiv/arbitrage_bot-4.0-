import os
import sys

# Ensure the project root (where engine/, cex_part/, dex_part/ live) is
# importable when pytest collects tests from tests/.
sys.path.insert(0, os.path.dirname(__file__))
