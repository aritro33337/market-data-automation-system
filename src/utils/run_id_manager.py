from pathlib import Path
import threading
from src.utils.logger import get_logger

class RunIdManager:
    _instance = None
    _lock = threading.RLock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(RunIdManager, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.logger = get_logger("RunIdManager")
        self.run_id_file = Path(".run_id")
        self._initialized = True

    def get_next_run_id(self, increment: bool = True) -> int:
        """
        Get the next run ID.
        If increment is True, the ID in the file is incremented.
        If increment is False, returns the current ID without changing it.
        """
        with self._lock:
            try:
                current_id = 0
                if self.run_id_file.exists():
                    try:
                        with open(self.run_id_file, "r") as f:
                            content = f.read().strip()
                            if content:
                                current_id = int(content)
                    except (ValueError, IOError):
                        current_id = 0
                
                if increment:
                    next_id = current_id + 1
                    with open(self.run_id_file, "w") as f:
                        f.write(str(next_id))
                    return next_id
                else:
                    return current_id if current_id > 0 else 1
            except Exception as e:
                self.logger.error(f"Error managing run ID: {str(e)}")
                return 1
