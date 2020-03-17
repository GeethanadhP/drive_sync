import io
import json

from googleapiclient.discovery import MediaFileUpload, build
from googleapiclient.http import MediaIoBaseDownload


class GoogleDrive:
    mime_types = {"folder": "application/vnd.google-apps.folder"}

    def __init__(self, creds):
        self.service = build("drive", "v3", credentials=creds)
        self.files = self.service.files()  # pylint: disable=no-member

    def _list_files(self, query):
        page_token = None
        file_list = []

        query = " ".join([x.strip() for x in query.strip().split("\n")])
        cols = "id, name, modifiedTime, mimeType, parents, md5Checksum"
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
            if data["mimeType"] == GoogleDrive.mime_types["folder"]:
                file_map[data["id"]] = DriveFolder(data)
            else:
                file_map[data["id"]] = DriveFile(data)

        for d_file in file_map.values():
            for pid in d_file.parents:
                file_map[pid].add_child(d_file)

        root.generate_child_paths()

    def download(self, file_id, path):
        req = self.files.get_media(fileId=file_id)
        with path.open("wb") as f:
            downloader = MediaIoBaseDownload(f, req)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                # print("Download %d%%." % int(status.progress() * 100))

    def upload(self, local_path, parent_id):
        file_metadata = {"name": local_path.name}

        media = MediaFileUpload(local_path, mimetype="image/jpeg")
        new_file = self.files.create(
            body=file_metadata, media_body=media, fields="id"
        ).execute()
        return new_file


class DriveFile:
    kind = "file"

    def __init__(self, data):
        self.id = data["id"]
        self.name = data["name"]
        self.parents = data.get("parents", [])
        self.path = None

    def json(self):
        return self.name

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.id == other.id


class DriveFolder(DriveFile):
    kind = "folder"

    def __init__(self, data):
        super(DriveFolder, self).__init__(data)
        self.children = []

    def add_child(self, child):
        self.children.append(child)

    def generate_child_paths(self):
        for child in self.children:
            child.path = self.path / child.name
            if child.kind == "folder":
                child.generate_child_paths()

    def json(self):
        return {self.name: [x.json() for x in self.children]}

    def pretty(self):
        return json.dumps(self.json(), indent=2)
