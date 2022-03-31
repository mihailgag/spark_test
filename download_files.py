import requests
import os
from bs4 import BeautifulSoup
from logger_internal import rootLogger as logger


class DownloadFiles:
    url = "https://datasets.imdbws.com/"

    def download_files(self):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, "lxml")
        gz_link_names = self.get_gz_links_from_url(soup)
        self.download_gz_files_and_get_names(gz_link_names)

    @staticmethod
    def get_gz_links_from_url(soup):
        all_links = soup.findAll("ul")
        return [link.a["href"] for link in all_links]

    def download_gz_files_and_get_names(self, links):
        return [self.download_gz_file(link) for link in links]

    @staticmethod
    def download_gz_file(url):
        logger.info(f"Downloading from link: {url}")
        filename = url.split("/")[-1]
        with open(filename, "wb") as f:
            r = requests.get(url)
            f.write(r.content)
        logger.info(f"Succesefully downloaded file: {filename}")
        return filename

    @staticmethod
    def remove_gz_file(file_name):
        current_dir = os.getcwd()
        file_path = current_dir + "\\" + file_name
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            logger.info("File doesn't exist")
