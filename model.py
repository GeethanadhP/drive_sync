import json
import os
import shutil
from datetime import datetime
from enum import Enum
from pathlib import Path

import pandas as pd
import yaml
from googleapiclient.discovery import MediaFileUpload, build
from googleapiclient.http import MediaIoBaseDownload

from utils import get_logger, get_md5

log = get_logger(__file__)


def get_local_df(root_path):
    file_list = []
    for root, dirs, files in os.walk(root_path):
        root = Path(root)
        for x in dirs:
            path = root / x
            file_list.append(
                {
                    "local_type": "folder",
                    "local_path": path,
                    "local_mtime": path.stat().st_mtime,
                }
            )
        for x in files:
            path = root / x
            file_list.append(
                {
                    "local_type": path.suffix.strip("."),
                    "local_path": path,
                    "local_mtime": max(path.stat().st_mtime, path.stat().st_ctime),
                    "local_md5": get_md5(path),
                }
            )
    return pd.DataFrame(file_list)


def delete_path(path):
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


class MimeType(Enum):
    FOLDER = "application/vnd.google-apps.folder"
    GDSHEET = "application/vnd.google-apps.spreadsheet"
    GDDOC = "application/vnd.google-apps.document"
    UNKNOWN = "application/vnd.google-apps.unknown"

    @classmethod
    def _missing_(cls, value):
        return MimeType.UNKNOWN


mime_types = {
    "application/vnd.google-apps.folder": "folder",
    "application/vnd.google-apps.spreadsheet": "gdsheet",
    "application/vnd.google-apps.document": "gddoc",
}


def mime_mapper(value):
    if value in mime_types:
        return mime_types[value]
    return "unknown"


class GoogleDrive:
    google_mimes = ["gdsheet", "gddoc"]
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

    def get_cloud_df(self, root_id, root_path):
        file_list = self.list_files(root_id)
        df = pd.DataFrame(file_list)
        df = df.set_index("id")
        df = df.rename(
            columns={
                "mimeType": "mime_type",
                "parents": "parent",
                "webViewLink": "url",
                "modifiedTime": "cloud_mtime",
                "md5Checksum": "cloud_md5",
                "name": "cloud_name",
            }
        )
        df["parent"] = df["parent"].apply(lambda x: x[0])
        df["cloud_mtime"] = pd.to_datetime(df["cloud_mtime"]).apply(
            lambda x: x.timestamp()
        )

        df["cloud_type"] = df["mime_type"].map(mime_mapper)
        df["local_name"] = df["cloud_name"]
        df["local_path"] = None
        google_mimes = df["cloud_type"].isin(GoogleDrive.google_mimes)
        df.loc[google_mimes, "local_name"] = df.loc[google_mimes].apply(
            lambda x: f"{x['cloud_name']}.{x['cloud_type']}", axis=1
        )

        folder_rows = df["cloud_type"] == "folder"

        def update_paths(base_id, base_path, root=False):
            child_rows = df["parent"] == base_id
            child_folder_rows = child_rows & folder_rows

            if child_rows.any():
                df.loc[child_rows, "local_path"] = df.loc[child_rows].apply(
                    lambda x: base_path / x["local_name"], axis=1
                )

            for id in df[child_folder_rows].index:
                update_paths(id, df.at[id, "local_path"])

        update_paths(root_id, root_path)
        return df

    def download(self, df):
        for path, row in df.iterrows():
            if row["cloud_type"] == "folder":
                path.mkdir(parent=True, exist_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                if row["cloud_type"] in GoogleDrive.google_mimes:
                    log.info(f"Linking `{path.name}`")
                    with path.open("w") as f:
                        json.dump(
                            {
                                "url": row["url"],
                                "account_email": self.email_address,
                                "file_id": row["id"],
                            },
                            f,
                        )
                else:
                    log.info(f"Downloading `{path.name}`")
                    req = self.files.get_media(fileId=row["id"])
                    with path.open("wb") as f:
                        downloader = MediaIoBaseDownload(f, req)
                        done = False
                        while done is False:
                            done = downloader.next_chunk()[1]

    def upload(self, df):
        for path, row in df.iterrows():
            if row["parent"] is None:
                raise RuntimeError(f"parent cannot be null for {path}")
            if row["cloud_type"] in GoogleDrive.google_mimes:
                log.warning(f"i have no idea what to do here mate {row['cloud_name']}")
            elif row["cloud_type"] == "folder":
                file_metadata = {
                    "name": row["cloud_name"],
                    "mimeType": mime_types["folder"],
                    "parents": [row["parent"]],
                }
                new_file = self.files.create(body=file_metadata, fields="id").execute()
            else:
                file_metadata = {"name": row["cloud_name"], "parents": [row["parent"]]}

                media = MediaFileUpload(path)
                new_file = self.files.create(
                    body=file_metadata, media_body=media, fields="id"
                ).execute()
            df.at[path, "id"] = new_file["id"]
        return df

    def delete(self, df):
        for _, row in df.iterrows():
            self.files.delete(fileId=row["id"]).execute()

    def sync(self, root_id, root_path, status):
        cloud_df = self.get_cloud_df(root_id, root_path)
        local_df = get_local_df(root_path)

        df = pd.merge(
            cloud_df.reset_index(),
            local_df,
            how="outer",
            indicator=True,
            on="local_path",
        ).set_index("local_path")

        cloud_extra = df["_merge"] == "left_only"
        to_download = cloud_extra & (df["cloud_mtime"] > status["start_time"])
        to_delete_cloud = cloud_extra & (df["cloud_mtime"] <= status["start_time"])

        local_extra = df["_merge"] == "right_only"
        df.loc[local_extra, "local_name"] = df.loc[local_extra].apply(
            lambda x: x.name.name, axis=1
        )
        df.loc[local_extra, "cloud_name"] = df.loc[local_extra].apply(
            lambda x: x.name.stem
            if x["local_type"] in GoogleDrive.google_mimes
            else x.name.name,
            axis=1,
        )
        to_upload = local_extra & (df["local_mtime"] > status["end_time"])
        to_delete_local = local_extra & (df["local_mtime"] <= status["end_time"])

        to_update = (
            (df["_merge"] == "both")
            & (df["local_md5"] != df["cloud_md5"])
            & (~df["cloud_type"].isin(GoogleDrive.google_mimes))
        )
        to_update_local = (
            to_update
            & (df["cloud_mtime"] > df["local_mtime"])
            & (df["local_type"] != "folder")
        )
        to_update_cloud = (
            to_update
            & (df["cloud_mtime"] < df["local_mtime"])
            & (df["local_type"] != "folder")
        )

        temp_df = df[to_download]
        if len(temp_df):
            log.info(f"{len(temp_df)} new files to download")
            self.download(temp_df)

        temp_df = df[to_update_local]
        if len(temp_df):
            log.info(f"{len(temp_df)} files updated in cloud, downloading them")
            self.download(temp_df)

        def find_parent(row):
            if row.name.parent == root_path:
                return root_id

            values = df.loc[df.index == row.name.parent]["id"].values
            if len(values):
                return values[0]
            else:
                return None

        df.loc[to_upload, "parent"] = df.loc[to_upload].apply(
            lambda x: find_parent(x), axis=1
        )

        # Upload folders first go get the id's to assign as parents
        temp_df = df[to_upload & (df["local_type"] == "folder")]
        if len(temp_df):
            log.info(f"{len(temp_df)} new folders to create")
            df.update(self.upload(temp_df))
            df.loc[to_upload, "parent"] = df.loc[to_upload].apply(
                lambda x: find_parent(x), axis=1
            )

        temp_df = df[to_upload & (df["local_type"] != "folder")]
        if len(temp_df):
            log.info(f"{len(temp_df)} new files to upload")
            df.update(self.upload(temp_df))

        temp_df = df[to_update_cloud & (df["local_type"] != "folder")]
        if len(temp_df):
            log.info(f"{len(temp_df)} files updated in local, uploading them to cloud")
            df.update(self.upload(temp_df))

        temp_df = df[to_delete_local]
        if len(temp_df):
            log.info(
                f"{len(temp_df)} files deleted in the cloud, deleting them in local"
            )
            for path in df[to_delete_local].index:
                log.info(f"Deleting `{path.name}`")
                delete_path(path)

        temp_df = df[to_delete_cloud]
        if len(temp_df):
            log.info(
                f"{len(temp_df)} files deleted in the local, deleting them in cloud"
            )
            self.delete(temp_df)

        # upload google mime types pending
