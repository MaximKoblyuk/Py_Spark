# Databricks notebook source
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests
from requests import Response

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ONE_MB = 1024 * 1024


def retrieve_token(client_id, client_secret, tenant, scope) -> str:
    """Retrieve token using MS login API

    Args:
        client_id (str): SPN client id
        client_secret (str): SPN client secret
        tenant (str): tenant
        scope (str): scope of the token to be retrieved

    Returns:
        requests.Response
    """
    body = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope,
        "grant_type": "client_credentials",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    get_token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    token = requests.post(get_token_url, data=body, headers=headers).json()
    return token["access_token"]


@dataclass(frozen=True)
class GraphClient:
    """Client to upload data via the Graph API. Class is based on
    https://github.com/RoyalAholdDelhaize/ah-sa-mijn-bonus-box/blob/bdded0d18e2db8adaf83f59b379678a138d09539/scripts/masterfile_loader.py

        Prerequisites that were met (with the help of Ahold Delhaize Administrators):
        - Service Principal (app registration) was added with read-access to the Sharepoint
        - Service Principal (app registration) was granted the API permissions:
        `Sites.Selected for Microsoft Graph`

    Returns
    -------
    GraphClient
        Client to interact with Graph API
    """

    GRAPH_API_URL: str = field(default="https://graph.microsoft.com/v1.0", init=False)
    tenant: str
    client_id: str
    client_secret: str
    session: requests.Session = field(default=requests.sessions.Session(), init=False)

    def __enter__(self) -> "GraphClient":
        self._init_session()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.session.close()

    def _init_session(self) -> None:
        """Initialize Graph API session by retrieving bearer token"""
        token = retrieve_token(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant=self.tenant,
            scope="https://graph.microsoft.com/.default",
        )

        headers = {"Authorization": f"Bearer {token}"}
        self.session.headers.update(headers)

    def get_site_id(self, sharepoint: str, site: str) -> str:
        """Returns id for a site on a specific sharepoint

        Parameters
        ----------
        sharepoint : str
            URL of the sharepoint (e.g. aholddelhaize.sharepoint.com)
        site : str
            Name of the site (e.g RSDCmrEngStoreOpsDataAnalytics)

        Returns
        -------
        str
            ID of the site
        """
        get_site = f"{self.GRAPH_API_URL}/sites/{sharepoint}:/sites/{site}"
        response = self.session.get(get_site).json()
        return response["id"]

    def get_drive_id(self, site_id: str) -> str:
        """Get the associated drive id of a specific site

        Parameters
        ----------
        site_id : str
            Site ID which can be fetched by get_site_id

        Returns
        -------
        str
            ID of the drive associated to a specific site
        """
        get_drive = f"{self.GRAPH_API_URL}/sites/{site_id}/drive"
        response = self.session.get(get_drive).json()
        return response["id"]

    def _validate_response(self, response: Response, data_type: str) -> bool:
        valid_response = True
        if response.ok:
            logger.info(
                f"Success, {data_type} can be found under {response.json()['webUrl']}."
            )
        else:
            logger.warning(
                f"Failed to create {data_type}. Status code: {response.status_code}, Reason: {response.reason}."
            )
            valid_response = False
        return valid_response

    def create_folder(self, drive_id: str, folder_path: Path) -> None:
        """Creates folder in a SharePoint with
        https://learn.microsoft.com/en-us/graph/api/driveitem-post-children

        Parameters
        ----------
        drive_id : str
            ID of the drive associated to the SharePoint site
        folder_path : Path
            Complete path where the folder should be created
        """
        folder_data = json.dumps({"name": folder_path.name, "folder": {}})
        create_folder = f"{self.GRAPH_API_URL}/drives/{drive_id}/root:/{folder_path.parent}:/children"
        headers = {"Content-Type": "application/json"}
        self.session.headers.update(headers)
        response = self.session.post(url=create_folder, data=folder_data)
        _ = self._validate_response(response=response, data_type="folder")

    def upload_small_file(
        self, drive_id: str, upload_folder: str, file_path: Path
    ) -> None:
        """Upload file to SharePoint (needs to be smaller than 4 MB)
        using https://learn.microsoft.com/en-us/graph/api/driveitem-put-content

        Parameters
        ----------
        file_path : Path
            File path to the object which should be uploaded
        """
        file_size = file_path.stat().st_size
        # check if file size is larger than 4 MB
        if file_size > ONE_MB * 4:
            logger.warning(
                f"File size {file_size} is larger than 4MB. Please use resumable_upload."
            )
            return

        upload_url = f"{self.GRAPH_API_URL}/drives/{drive_id}/root:/{upload_folder}/{file_path.name}:/content"
        file_as_bytes = file_path.read_bytes()
        response = self.session.put(url=upload_url, data=file_as_bytes)
        _ = self._validate_response(response=response, data_type="file")

    def _create_upload_session(
        self, drive_id: str, upload_path: str, file_path: Path
    ) -> str:
        """Start an upload session for a resumable upload and returns the upload URL which should be used

        Parameters
        ----------
        drive_id : str
            Id of the drive associated to the site
        upload_path : str
            Complete path where file should be uploaded on the SharePoint drive
        file_path : Path
            Local file path

        Returns
        -------
        str
            Upload URL for session
        """
        upload_session_data = json.dumps({"item": {"name": file_path.name}})
        self.session.headers.update({"Content-Type": "application/json"})
        upload_session_url = f"{self.GRAPH_API_URL}/drives/{drive_id}/root:/{upload_path}:/createUploadSession"
        response = self.session.post(
            url=upload_session_url, data=upload_session_data
        ).json()
        return response["uploadUrl"]

    def _splitted_upload(
        self,
        upload_url: str,
        data: bytes,
        upload_size: int,
        file_size: int,
        start_byte: int,
    ) -> Response:
        """Uploads a byte chunk using a precreated upload session

        Parameters
        ----------
        upload_url : str
            Upload URL created by the upload session
        data : bytes
            Byte chunk which will be uploaded
        upload_size : int
            Size of the byte chunk
        file_size : int
            Total size of the file to upload
        start_byte : int
            Start byte within the file

        Returns
        -------
        Response
            Response of the upload
        """
        headers = {
            "Content-Length": str(upload_size),
            "Content-Range": f"bytes {start_byte}-{start_byte + upload_size-1}/{file_size}",
        }
        response_upload = requests.put(url=upload_url, data=data, headers=headers)

        return response_upload

    def resumable_upload(
        self, drive_id: str, upload_folder: str, file_path: Path
    ) -> None:
        """Upload files larger than 4MB and up to 60 MB (splitted upload is not supported yet)
        https://learn.microsoft.com/en-us/graph/api/driveitem-createuploadsession

        Parameters
        ----------
        drive_id : str
            Id of the drive associated to the site
        upload_folder : str
            Folder path
        file_path : Path
            Local file path
        """

        file_size = file_path.stat().st_size

        upload_url = self._create_upload_session(
            drive_id=drive_id,
            upload_path=f"{upload_folder}/{file_path.name}",
            file_path=file_path,
        )

        file_as_bytes = file_path.read_bytes()

        UPLOAD_BUFFER_SIZE = ONE_MB * 50

        complete_chunks = file_size // UPLOAD_BUFFER_SIZE
        left_chunk = file_size % UPLOAD_BUFFER_SIZE

        for i in range(complete_chunks):
            response_upload = self._splitted_upload(
                upload_url=upload_url,
                data=file_as_bytes[
                    i * UPLOAD_BUFFER_SIZE : (i + 1) * UPLOAD_BUFFER_SIZE
                ],
                upload_size=UPLOAD_BUFFER_SIZE,
                file_size=file_size,
                start_byte=i * UPLOAD_BUFFER_SIZE,
            )
            if not response_upload.ok:
                _ = self.session.delete(url=upload_url)
                logger.warning("Upload session will now be canceled.")
                return
        else:
            response_upload = self._splitted_upload(
                upload_url=upload_url,
                data=file_as_bytes[complete_chunks * UPLOAD_BUFFER_SIZE :],
                upload_size=left_chunk,
                file_size=file_size,
                start_byte=complete_chunks * UPLOAD_BUFFER_SIZE,
            )

            valid_response = self._validate_response(
                response=response_upload, data_type="file"
            )
        if not valid_response:
            _ = self.session.delete(url=upload_url)
            logger.warning("Upload session will now be canceled.")

    def delete_file(self, drive_id: str, file_path: str) -> None:
        """Delete file on SharePoint drive

        Parameters
        ----------
        drive_id : str
            Id of the drive associated to the site
        file_path : str
            File path on the SharePoint drive
        """
        delete_url = f"{self.GRAPH_API_URL}/drives/{drive_id}/root:/{file_path}"
        response_delete = self.session.delete(url=delete_url)
        if response_delete.status_code == 204:
            logger.info(f"Successfully deleted file: {file_path}")
        else:
            logger.warning(f"Not able to delete file: {file_path}")

    def download_file(self, drive_id: str, remote_path: str, local_path: str):

        file_url = (
            f"{self.GRAPH_API_URL}/drives/{drive_id}/root:/{remote_path}:/content"
        )

        response = self.session.get(file_url)
        with open(local_path, "wb") as file:
            file.write(response.content)

    def list_dir(self, drive_id: str, dir_path: str):
        dir_url = f"{self.GRAPH_API_URL}/drives/{drive_id}/root:/{dir_path}:/children"

        return self.session.get(dir_url)

# COMMAND ----------

CLIENT_ID = "28a1dc32-6cf7-4313-a439-1ab977910413"
CLIENT_SECRET = dbutils.secrets.get(scope="akv-secrets", key="secret-fbn-spn")
TENANT_ID = "a6b169f1-592b-4329-8f33-8db8903003c7"

SITE = "ACZDataLakehouse"

FOLDER_PATH = "API_ACCESS/demo"
small_file = Path("sharepoint_demo_data.csv")
big_file = Path("sharepoint_demo_data_large.csv")

# COMMAND ----------

# DBTITLE 1,Upload small file < 4MB
with GraphClient(
    tenant=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
) as client:
    site_id = client.get_site_id(sharepoint="aholddelhaize.sharepoint.com", site=SITE)
    drive_id = client.get_drive_id(site_id=site_id)

    client.create_folder(drive_id=drive_id, folder_path=Path(FOLDER_PATH))

    client.upload_small_file(
        drive_id=drive_id, upload_folder=FOLDER_PATH, file_path=small_file
    )

# COMMAND ----------

# DBTITLE 1,Upload big file 4MB < size < ??? MB
with GraphClient(
    tenant=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
) as client:
    site_id = client.get_site_id(sharepoint="aholddelhaize.sharepoint.com", site=SITE)
    drive_id = client.get_drive_id(site_id=site_id)

    # client.create_folder(drive_id=drive_id, folder_path=Path(FOLDER_PATH))

    client.resumable_upload(
        drive_id=drive_id, upload_folder=FOLDER_PATH, file_path=big_file
    )

# COMMAND ----------

# MAGIC %md
# MAGIC https://aholddelhaize.sharepoint.com/sites/ACZDataLakehouse/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FACZDataLakehouse%2FShared%20Documents%2FAPI%5FACCESS%2Fdemo

# COMMAND ----------

# with open("sharepoint_demo_data.csv", "r") as file:
#     c = file.read()
# with open("sharepoint_demo_data_large.csv", "w") as file:
#     file.write(c * 230_000)

# COMMAND ----------

!ls -lah

# COMMAND ----------

# DBTITLE 1,Download file
with GraphClient(
    tenant=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
) as client:
    site_id = client.get_site_id(sharepoint="aholddelhaize.sharepoint.com", site=SITE)
    drive_id = client.get_drive_id(site_id=site_id)

    client.download_file(
        drive_id=drive_id,
        remote_path="API_ACCESS/demo/template_out.json",
        local_path="template_out.json",
    )

# COMMAND ----------


