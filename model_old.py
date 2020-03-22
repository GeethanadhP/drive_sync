import json
from enum import Enum
from pathlib import Path

import yaml
from googleapiclient.discovery import MediaFileUpload, build
from googleapiclient.http import MediaIoBaseDownload

from utils import get_logger, get_md5

log = get_logger(__file__)


class MimeType(Enum):
    FOLDER = "application/vnd.google-apps.folder"
    GDSHEET = "application/vnd.google-apps.spreadsheet"
    GDDOC = "application/vnd.google-apps.document"
    UNKNOWN = "application/vnd.google-apps.unknown"

    @classmethod
    def _missing_(cls, value):
        return MimeType.UNKNOWN


class GoogleDrive:
    link_mimes = [MimeType.GDSHEET, MimeType.GDDOC]
    non_md5_mimes = link_mimes + [MimeType.FOLDER]

    def __init__(self, creds):
        self.service = build("drive", "v3", credentials=creds)
        self.email_address = (
            self.service.about()  # pylint: disable=no-member
            .get(fields="user(emailAddress)")
            .execute()["user"]["emailAddress"]
        )

        self.files = self.service.files()  # pylint: disable=no-member

    def _list_files(self, query):
        page_token = None
        file_list = []

        query = " ".join([x.strip() for x in query.strip().split("\n")])
        cols = "id, name, modifiedTime, mimeType, parents, md5Checksum, webViewLink"
        fields = f"nextPageToken, files({cols})"
        while True:
            resp = self.files.list(
                spaces="drive", fields=fields, pageToken=page_token, q=query,
            ).execute()
            file_list.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken", None)
            if page_token is None:
                break

        return file_list

    def list_all_files(self):
        query = "('me' in owners) and (trashed=false)"
        return self._list_files(query)

    def list_files(self, root_id):
        query = "('me' in owners) and (trashed=false)"
        return self._list_files(query)

    def get_root(self):
        return self.files.get(fileId="root").execute()

    def generate_tree(self, root):
        file_list = self.list_files(root.id)
        file_map = {root.id: root}

        for data in file_list:
            file_map[data["id"]] = DriveFile.cloud(data)

        for d_file in file_map.values():
            for pid in d_file.parents:
                file_map[pid].add_child(d_file)

        root.generate_paths(base_path=None)

    def download(self, drive_file):
        if drive_file.mime_type is MimeType.FOLDER:
            for child in drive_file.children:
                self.download(child)
            return

        drive_file.path.parent.mkdir(parents=True, exist_ok=True)
        if drive_file.mime_type in GoogleDrive.link_mimes:
            if drive_file.path.exists():
                log.debug(f"File `{drive_file.path.name}` already linked")
                return

            log.info(f"Linking `{drive_file.path.name}`")
            with drive_file.path.open("w") as f:
                json.dump(
                    {
                        "url": drive_file.link,
                        "account_email": self.email_address,
                        "file_id": drive_file.id,
                    },
                    f,
                )
        else:
            if drive_file.path.exists():
                if get_md5(drive_file.path) == drive_file.md5:
                    log.debug(f"File `{drive_file.path.name}` already matching")
                    return
                else:
                    log.info(f"File `{drive_file.path.name}` updated in cloud")

            log.info(f"Downloading `{drive_file.path.name}`")
            req = self.files.get_media(fileId=drive_file.id)
            with drive_file.path.open("wb") as f:
                downloader = MediaIoBaseDownload(f, req)
                done = False
                while done is False:
                    done = downloader.next_chunk()[1]

    def upload(self, drive_file):
        file_metadata = {"name": drive_file.name, "parents": drive_file.parents}

        media = MediaFileUpload(drive_file.path)
        new_file = self.files.create(
            body=file_metadata, media_body=media, fields="id"
        ).execute()
        return new_file


class DriveFile:
    def __init__(self):
        self.id = None
        self.name = None
        self.mime_type = None
        self.full_name = None
        self.link = None
        self.md5 = None
        self.parents = []
        self.path = None
        self.children = []

    @staticmethod
    def cloud(data):
        self = DriveFile()
        self.id = data["id"]
        self.name = data["name"]
        self.mime_type = MimeType(data["mimeType"])

        if self.mime_type in GoogleDrive.link_mimes:
            self.full_name = f"{self.name}.{self.mime_type.name.lower()}"
            self.link = data["webViewLink"]
        else:
            self.full_name = self.name

        if self.mime_type not in GoogleDrive.non_md5_mimes:
            self.md5 = data["md5Checksum"]

        self.parents = data.get("parents", [])

    @staticmethod
    def local(root, path):
        self = DriveFile()
        path = Path(path)
        parent = root.find_parent(path)
        if parent:
            self.parents.append(parent.id)
        self.full_name = path.name
        self.path = path
        self.name = self.full_name

    def find_parent(self, path):
        rel_path = path.relative_to(self.path)
        if len(rel_path.parts) > 1:
            parent = None
            for child in self.children:
                if child.name == rel_path.parts[0]:
                    parent = child.find_parent(path)
                    break
        else:
            parent = self
        return parent

    def add_child(self, child):
        if self.mime_type is not MimeType.FOLDER:
            raise RuntimeError(f"Can't add child to {self.full_name}, its not a folder")
        self.children.append(child)

    def generate_paths(self, base_path):
        if base_path:
            if type(base_path) == str:
                base_path = Path(base_path)

            self.path = base_path / self.full_name
        if self.mime_type is MimeType.FOLDER:
            for child in self.children:
                child.generate_paths(base_path=self.path)

    def json(self):
        if self.mime_type is MimeType.FOLDER:
            return {self.name: [x.json() for x in self.children]}
        else:
            return self.full_name

    def pprint(self):
        print(yaml.dump(self.json()))

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.id == other.id
