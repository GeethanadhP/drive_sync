import os
from pathlib import Path

from yaml import safe_load

from auth import get_creds
from model import DriveFolder, GoogleDrive

data_path = Path.home() / ".drive_sync"

config_path = data_path / "config.yaml"
status_path = data_path / "status"

with config_path.open() as f:
    config = safe_load(f)

for account, account_config in config["gdrive"].items():
    count = 0
    while True:
        count += 1
        creds, success = get_creds(account)
        if success:
            break
        if count > 3:
            raise RuntimeError(f"{account} is not logged in")

    drive = GoogleDrive(creds)
    root = DriveFolder(drive.get_root())
    root.path = account_config['target']
    
    drive.generate_tree(root)

