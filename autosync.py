import getpass
import shutil
import subprocess
import time
from pathlib import Path
import yaml
from watchdog.observers import Observer
from logger import logger
from utils import DBFSHandler, path_is_valid

with open("config.yaml", "r") as f:
    cfg = yaml.load(f, Loader=yaml.FullLoader)


local_root = Path(cfg["local_root"])
c_number = getpass.getuser()
dbfs_root = f"dbfs:/FileStore/packages/{c_number}/{local_root.name}/"
temp_path = Path.cwd().joinpath("temp")
recursive = cfg.get('recursive', True)
profile = cfg.get("profile", "DEFAULT")


logger.debug("Cleaning dbfs directory before initial copy.")
if dbfs_root.startswith("dbfs:/FileStore/packages/"):
    subprocess.run(f'dbfs rm -r "{dbfs_root}" --profile {profile}')
else:
    raise Exception(f"dbfs root {dbfs_root} not allowed. Must start with 'dbfs:/FileStore/packages/...'.")

# Initial copy. First move all files to a temp folder and then recursively copy that folder to dbfs. This uses 1 API call to dbfs
# and is much faster than making seperate calls for every file.
try:
    logger.debug(f"Initial copy, copying all files to one folder to copy recursively to dbfs (faster).")
    logger.debug(f"Copying files to {temp_path}, this temp folder will be removed after initial copy.")

    if recursive:
        paths = [Path(p) for p in local_root.rglob("*") if path_is_valid(p)]
    else:
        paths = [Path(p) for p in local_root.glob("*") if path_is_valid(p)]
        
    if not paths:
        message = f"Could not sync. No files that match pattern criteria in {local_root}. Please change the patterns in config.yaml or remove them."
        raise Exception(message)
            
    for path in paths:
        relative_path = path.relative_to(local_root)
        target_path = temp_path.joinpath(relative_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target_path)

    
    logger.debug(f"Writing initial copy to {dbfs_root} for profile {profile}.")
    subprocess.run(f'dbfs cp -r "{temp_path}" "{dbfs_root}" --overwrite --profile {profile}')

finally:
    shutil.rmtree(temp_path)
    


logger.debug(
    f"""
    \n
    Add the following code at the top of your databricks notebooks to keep everything synced automatically.
    You can then use 'from {Path(local_root).name} import ... to use your modules in a Databricks notebook.

    Cell 1:
    %load_ext autoreload
    %autoreload 2

    Cell 2:
    import sys
    sys.path.insert(0, '/{dbfs_root.replace(":", "")}')
    \n
"""
)

logger.debug("Starting autosync..")
if __name__ == "__main__":
    event_handler = DBFSHandler()
    observer = Observer()
    observer.schedule(event_handler, path=local_root, recursive=recursive)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
