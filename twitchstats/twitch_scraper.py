import yaml
import os

class TwitchScraper:
    def __init__(self):
        os.chdir('..')
        with open('keys.yml') as f:
            keys = yaml.load(f)
            self.client_id = keys['twitchclientid']


if __name__ == "__main__":
    a = TwitchScraper()
    print("Done.")