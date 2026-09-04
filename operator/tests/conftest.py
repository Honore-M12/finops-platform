import os
import sys

# Permet `import handlers` depuis operator/handlers.py sans packaging,
# en ajoutant le dossier parent (operator/) au sys.path.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
