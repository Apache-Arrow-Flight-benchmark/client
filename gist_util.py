import requests
from datetime import datetime
from dotenv import load_dotenv
import time
import os

class GistUtil:
    def __init__(self):
        load_dotenv()
        self.token = os.getenv("GITHUB_TOKEN")
        self.headers = {"Authorization": f"token {self.token}"}
        self.public = False
        self.api_url = "https://api.github.com/gists"

    def upload_results(self, results, desctiptors):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        description = f"timestamp: {timestamp}, "
        for key, value in desctiptors.items():
            description += f"{key}: {value}, "

        fetch_time = ""
        latency = ""
        for res in results:
            if "fetch_time" in res:
                fetch_time += f"{res['fetch_time']}\n"
            if "latency" in res:
                latency += f"{res['latency']}\n"
        
        if len(fetch_time) > 0:
            self.upload("fetch_time.txt", description, fetch_time)
        if len(latency) > 0:
            self.upload("latency.txt", description, latency)

        print("Files uploaded successfully.")
        
    def upload(self, filename, description, content):
        payload = {
            "description": description,
            "public": self.public,
            "files": {
                filename: {
                    "content": content
                }
            }
        }
        response = requests.post(self.api_url, headers=self.headers, json=payload)
        if response.status_code == 201:
            url = response.json()["html_url"]
        else:
            print(f"Failed to create gist: {response.status_code} - {response.text}")

    def list(self):
        page = 1
        all_gists = []
        print("Fetching all gists...")
        while True:
            response = requests.get(self.api_url, headers=self.headers, params={"page": page, "per_page": 100})
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                break

            gists = response.json()
            if not gists:
                break

            all_gists.extend(gists)
            page += 1
        return all_gists

    def delete_all(self):
        page = 1
        deleted = 0
        while True:
            response = requests.get(self.api_url, headers=self.headers, params={"page": page, "per_page": 100})
            if response.status_code != 200:
                print(f"Error fetching gists: {response.status_code}")
                break

            gists = response.json()
            if not gists:
                break

            for gist in gists:
                gist_id = gist["id"]
                desc = gist.get("description", "")
                del_response = requests.delete(f"{self.api_url}/{gist_id}", headers=self.headers)
                if del_response.status_code == 204:
                    print(f"Deleted: {gist_id} - {desc}")
                    deleted += 1
                else:
                    print(f"Failed to delete {gist_id}: {del_response.status_code}")
            page += 1

        print(f"\nDeleted {deleted} gists.")
