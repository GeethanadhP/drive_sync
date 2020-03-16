import json

from googleapiclient.discovery import build


class GoogleDrive:
    mime_types = {"folder": "application/vnd.google-apps.folder"}

    def __init__(self, creds):
        self.drive = build("drive", "v3", credentials=creds)
        self.files = self.drive.files()  # pylint: disable=no-member

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

    def list_files(self):
        query = "('me' in owners) and (trashed=false)"
        return self._list_files(query)

    def get_root(self):
        return self.files.get(fileId="root").execute()

    def list_root_files(self):
        query = f"""
            ('me' in owners)
            and (trashed = false)
            and ('root' in parents)
        """
        return self._list_files(query)

    def generate_structure(self, file_list):
        root_folder = DriveFolder(self.get_root())
        file_map = {root_folder.id: root_folder}

        for data in file_list:
            if data["mimeType"] == GoogleDrive.mime_types["folder"]:
                file_map[data["id"]] = DriveFolder(data)
            else:
                file_map[data["id"]] = DriveFile(data)

        for d_file in file_map.values():
            for pid in d_file.parents:
                file_map[pid].add_child(d_file)

        return root_folder


class DriveFile:
    kind = "file"

    def __init__(self, data):
        self.id = data["id"]
        self.name = data["name"]
        self.parents = data.get("parents", [])

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

    def json(self):
        return {self.name: [x.json() for x in self.children]}

    def pretty(self):
        return json.dumps(self.json(), indent=2)
