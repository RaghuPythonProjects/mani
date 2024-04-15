import os
from datetime import datetime, timedelta
import requests
from sharepoint import LegacySharepoint
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class SharePointAPI:
    """
        This class manages the publication of files to SharePoint. It is designed to handle
        the authentication with SharePoint, creation of necessary folders, and uploading files.
    """
    def __init__(self):
        """Initializes the InventorySharePointAPI class with predefined SharePoint connection settings."""
        self.sp = self._sharepoint()
        self.upload_timeout_limit = 180

    def _sharepoint(self):
        """
            encapsulates the SharePoint configuration details to abstract the connection setup process.
            Create and return an instance of the LegacySharepoint.
        """
        # SharePoint configuration details
        client_id = "5cab18b8-aa91-4cf8-a3bc-02d16b59d804"
        client_secret = "mnk/XjcBgeFNWBsgl9pbMcHG3ZRTIbkkyGxIYSXfz4U="
        tenant_id = "9c7d751b-b1a4-4c4a-9343-12b2814ae031"
        target_host = "otiselevator.sharepoint.com"
        base_url = "https://otiselevator.sharepoint.com/teams/VMCollaboration/"
        return LegacySharepoint(client_id, client_secret, tenant_id, target_host, base_url)

    def publish_files(self, folder, reports):
        """
        Publishes the specified report files to a SharePoint folder.

        Parameters:
            folder (str): The SharePoint folder path where files should be published.
            reports (list[str]): A list of file paths to the report files to be published.
        """
        folder_url = self._find_or_create_target_folder(folder)
        if folder_url:
            self._upload_files_to_folder(folder_url, reports)

    def _find_or_create_target_folder(self, path):
        """
            Finds or creates the folder at the specified path on SharePoint.

            Parameters:
                path (str): The server-relative URL path to the SharePoint folder.

            Returns:
                str|None: The full URL to the folder if successful, None if an error occurred.
        """
        print(f"Attempting to path to {path}")
        path_array = path.split('/')
        for i in range(2, len(path_array) + 1):
            current_path = '/'.join(path_array[:i])
            print(f"Verifying existence of {current_path}")
            response = self.sp.check_folder_exists(current_path)
            if not response.json().get("d", {}).get("Exists", False):
                try:
                    self.sp.create_folder(current_path)
                except Exception as e:
                    print(f"ERROR: {e}")
                    return None
        return f"{self.sp.base_url}_api/web/GetFolderByServerRelativeUrl('{path}')/"

    def _upload_files_to_folder(self, url, files):
        """
            Uploads files to the specified SharePoint folder.

            Parameters:
                url (str): The URL to the SharePoint folder where files should be uploaded.
                files (list[str]): A list of file paths to upload.
        """
        for file in files:
            with open(file, 'rb') as f:
                contents = f.read()
            file_name = os.path.basename(file)
            result = self.sp.upload_file(url, file_name, contents, self.upload_timeout_limit)
            if result is not None:
                print(f"Upload result: {result.status_code} - {result.reason}")

def get_history_folder_path(folder):
    now = datetime.now()
    current_monday = now - timedelta(days=now.weekday())
    target_date = current_monday.strftime("%Y-%m-%d")
    target_year = current_monday.year

    return f"Shared Documents/Weekly Reporting/{target_year}/{target_date}/{folder}"

def get_latest_folder_path(folder):
    return f"Shared Documents/Weekly Reporting/!LATEST-Weekly-Reports/{folder}"

def get_report_paths(folder):
    path_to_folder = os.path.join(os.getcwd(), folder)
    return [os.path.join(path_to_folder, f) for f in os.listdir(path_to_folder)]

if __name__ == "__main__":
    output_path = "Output_HI"

    report_paths = get_report_paths(output_path)
    latest_folder = get_latest_folder_path("!LATEST-New-Inventory-Reports")
    history_folder = get_history_folder_path("High Impact System")

    sharepoint_api = SharePointAPI()
    sharepoint_api.publish_files(folder=history_folder, reports=report_paths)
