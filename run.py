
import subprocess
import sys
import os

# Always run Main.py using the .venv Python interpreter
folder = os.path.dirname(os.path.abspath(__file__))
venv_python = os.path.join(folder, '.venv', 'bin', 'python')
main_script = os.path.join(folder, 'Main.py')

if not os.path.exists(venv_python):
    print("ERROR: .venv not found. Please set up the virtual environment first.")
    input("Press Enter to close...")
    sys.exit(1)

subprocess.run([venv_python, main_script])
