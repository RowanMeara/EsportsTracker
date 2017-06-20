import time
import urllib3
import os
import sys
import apiclient
import google.auth
import google.oauth2 as oauth2

DEBUG = True

class YoutubeScraper:
    def __init__(self):
        self.placeholder = 0
        self.youtube = self.authenticate_youtube("placeholder")

    def authenticate_youtube_api(self, api_key):
        """

        :param api_key: str: youtube API key
        :return: youtube API flow instance.
        """
        flow =

if __name__ == "__main__":
    a = YoutubeScraper()
    while True:
        start_time = time.time()
        # a.scrape_top_games()
        a.scrape_esports_channels('League of Legends')
        if DEBUG:
            print("Elapsed time: {:.2f}s".format(time.time() - start_time))
        time.sleep(300 - (time.time() - start_time))
