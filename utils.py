# %%
import getpass
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
import yaml
from watchdog.events import FileSystemEventHandler
from logger import logger
from pprint import pformat

with open("config.yaml", "r") as f:
    cfg = yaml.load(f, Loader=yaml.FullLoader)


# When a file is modified, it creates two events. We keep track of a time difference between
# two events to prevent duplicate triggers.
event_cache = {}
MIN_EVENT_TIME_DIFFERENCE = 1

local_root = Path(cfg['local_root'])
c_number = getpass.getuser()
dbfs_root = f"dbfs:/FileStore/packages/{c_number}/{local_root.name}/"

def pattern_matches(string, patterns):
    """Checks if a string matches a list of patterns. Returns a list of booleans,
    one for each pattern.
    """
    return [p.search(string) for p in map(re.compile, patterns)]


def path_is_valid(string):
    """Checks if the path matches the patterns. If it's valid, it will be created, updated or deleted
    on dbfs. If not, the file is ignored."""

    # Convert to string with forward slashes.
    string = Path(string).as_posix()

    patterns = cfg.get('patterns', {})

    or_patterns = patterns.get('or')
    if or_patterns and not any(pattern_matches(string, or_patterns)):
        return False

    and_patterns = patterns.get('and')
    if and_patterns and not all(pattern_matches(string, and_patterns)):
        return False

    nor_patterns = patterns.get('nor')
    if nor_patterns and any(pattern_matches(string, nor_patterns)):
        return False

    nand_patterns = patterns.get('nand')
    if nand_patterns and all(pattern_matches(string, nand_patterns)):
        return False

    return True


def event_is_valid(event):
    """Checks whether the event is valid
        - if it's not the same event triggered between a small time interval
        - if the path matches the patterns
    """
    global event_cache

    last_event_time = event_cache.get(event.src_path, 0)
    valid_time = (time.time() - last_event_time) > MIN_EVENT_TIME_DIFFERENCE
    valid_path = path_is_valid(event.src_path)

    if valid_time and valid_path:
        return True
    else:
        return False

class DBFSHandler(FileSystemEventHandler):
    """File handler class to define actions when file events take place.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.profile = cfg.get('profile', 'DEFAULT')
        
   

    def on_modified(self, event):
        """When a local file is modified locally, copy-overwrite it to dbfs."""
       
        if event_is_valid(event):
            
            src_path = Path(event.src_path)
            relative_path = src_path.relative_to(local_root)
            dbfs_path = f"{dbfs_root}/{relative_path}".replace("\\", "/")

            logger.debug(f"""\n
                            Updating {src_path.name}.
                            source: {src_path}
                            target: {dbfs_path} """)
            subprocess.run(f'dbfs cp "{src_path}" "{dbfs_path}" --overwrite --profile {self.profile}')
            logger.debug(f"Done!")

            global event_cache
            event_cache[event.src_path] = time.time()


    def on_created(self, event):
        """When a file is created locally, copy it to dbfs."""
        self.on_modified(event)
        
  
    def on_deleted(self, event):
        """When a file is deleted locally, it will be removed on dbfs."""
        if event_is_valid(event):

            src_path = Path(event.src_path)
            relative_path = src_path.relative_to(local_root)
            dbfs_path = f"{dbfs_root}/{relative_path}".replace("\\", "/")

            logger.debug(f"dbfs_path: {dbfs_path}")

            logger.debug(f"""Updating {src_path.name}.
                            source: {src_path}
                            target: {dbfs_path} """)
            subprocess.run(f'dbfs rm "{dbfs_path}" --profile {self.profile}')
            logger.debug(f"Done!")

            global event_cache
            event_cache[event.src_path] = time.time()
