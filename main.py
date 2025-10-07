"""Entry point for the Sunseeker data toolkit application."""
from __future__ import annotations

import sys
import os
import struct
import datetime as _dt
from typing import Callable, List, Dict, Any, Optional, Tuple
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import List, Optional
import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
from pathlib import Path
from typing import List, Optional
from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtGui import QFont
import pandas as pd
import warnings
from matplotlib import cm
from PyQt6.QtCore import Qt
import numpy as np
import time

from app.main_window import launch_app


if __name__ == "__main__":
    launch_app()
