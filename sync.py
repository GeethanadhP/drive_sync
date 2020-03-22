import json
from datetime import datetime
from pathlib import Path

from yaml import safe_load

from auth import get_creds
from model import GoogleDrive
from utils import get_logger

log = get_logger(__name__)
data_path = Path.home() / ".drive_sync"

config_path = data_path / "config.yaml"

with config_path.open() as f:
    config = safe_load(f)


def load_status(status_path):
    if status_path.exists():
        with status_path.open() as f:
            status = json.load(f)
    else:
        status = {"start_time": 1, "end_time": 1}
    return status


def save_status(status_path, status):
    with status_path.open("w") as f:
        json.dump(status, f)


def main():
    for service, service_config in config.items():
        for account, account_config in service_config.items():
            status_path = data_path / f"{service}_{account}.status"

            status = load_status(status_path)

            log.info(f"Logging into `{service}` account `{account}`")
            count = 0
            while True:
                count += 1
                creds, success = get_creds(account)
                if success:
                    break
                if count > 3:
                    raise RuntimeError(f"{account} is not logged in")

            drive = GoogleDrive(creds)
            root_id = drive.get_root()["id"]
            root_path = Path(account_config["target"])

            start_time = datetime.now().timestamp()
            log.info(f"Syncing `{service}` account `{account}`")
            drive.sync(root_id=root_id, root_path=root_path, status=status)
            log.info(f"Completed Syncing `{service}` account `{account}`")
            end_time = datetime.now().timestamp()

            status["start_time"] = start_time
            status["end_time"] = end_time
            save_status(status_path, status)


if __name__ == "__main__":
    main()
