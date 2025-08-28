#%% init
# from json import load, dumps
from pathlib import Path

## Logger setup
import logging
pymongo_logger = logging.getLogger('pymongo')
# Set its level to WARNING to silence INFO and DEBUG messages
pymongo_logger.setLevel(logging.WARNING)
class LoggerManager:
    """Singleton Logger Manager for consistent logging across the app."""
    _logger = None

    @classmethod
    def get_logger(cls, log_dir="logs/", name=__name__, level="DEBUG"):
        if cls._logger is None:
            log_dir = Path(log_dir)
            log_dir.mkdir(exist_ok=True)
            log_file = log_dir / f"{name}.log"
            logging.basicConfig(
                level=logging.DEBUG,
                # also log the calling function name
                format='%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s',
                # datefmt='%Y-%m-%d %H:%M:%S',
                handlers=[
                    logging.FileHandler(log_file, encoding="utf-8"),
                    logging.StreamHandler()
                ]
            )
            # set logging level
            if isinstance(level, str):
                level = getattr(logging, level.upper(), logging.INFO)
            cls._logger = logging.getLogger(name)
            cls._logger.setLevel(level)
        return cls._logger

lg = LoggerManager.get_logger(__name__)

class FileManager:
    """Singleton File Manager for consistent file operations."""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FileManager, cls).__new__(cls)
        return cls._instance

    # @staticmethod
    # def load_json_file(filename):
    #     """Load and return data from a JSON file."""
    #     with open(filename, 'r') as file:
    #         return load(file)

    # @staticmethod
    # def write_json_file(filename, data):
    #     """Write data to a JSON file."""
    #     with open(filename, 'w') as file:
    #         file.write(dumps(data, indent=4, ensure_ascii=False))
    
    @staticmethod
    def read_creds(filename="mongo_creds.txt"):
        """Read MongoDB credentials from a file using pathlib"""
        if not Path(filename).exists():
            _ERROR_MSG = f"Credentials file {filename} does not exist.Please create a mongo_creds.txt with user:passwd"
            lg.error(_ERROR_MSG)
            raise FileNotFoundError(_ERROR_MSG)
        user, passwd = Path(filename).read_text().strip().split(":")
        lg.debug("MongoDB credentials loaded successfully.")
        return user, passwd


#%% Testing
# lg.WARNING
# import LoggerManager as lm
# logging.WARNING
# %%
