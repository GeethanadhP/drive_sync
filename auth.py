import os
import pickle
from pathlib import Path

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_creds(email):
    creds = None
    same_mail = True

    token_path = Path(f"{email}.gdrive")
    if token_path.exists():
        with token_path.open("rb") as f:
            creds = pickle.load(f)

    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        service = build("drive", "v3", credentials=creds)
        user_email = (
            service.about()  # pylint: disable=no-member
            .get(fields="user(emailAddress)")
            .execute()["user"]["emailAddress"]
        )
        if user_email != email:
            same_mail = False

        token_path = Path(f"{user_email}.gdrive")
        # Save the credentials for the next run
        with token_path.open("wb") as f:
            pickle.dump(creds, f)
    return (creds, same_mail)
