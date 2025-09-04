from pathlib import Path
from sys import platform

DB_NAME = "iris-db"
DB_IP = "10.171.18.104"
if platform == "linux":
    DB_IP = "localhost"
# DB_IP = "localhost"


## Change as needed
# DS_PREFIX = Path("D:/").expanduser()
# if platform == "linux":
    # DS_PREFIX = Path("/home/abhishek/datasets").expanduser()
    # DS_PREFIX = Path("~/datasets/fvc_fingerprint_datasets").expanduser()

## List of Available datasets
# AVAIL_DS = {_.name for _ in DS_PREFIX.iterdir() if _.is_dir()}



# DB_CON = f"mongodb://{DB_IP}/{DB_NAME}"
