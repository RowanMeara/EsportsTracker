from ruamel import yaml


class Aggregator:
    def __init__(self, config_path='scraper_config.yml'):
        self.aggregation_interval = 3600
        self.config_path = config_path
        with open(config_path) as f:
            config = yaml.safe_load(f)
        self.twitch_db = config['twitch']['db']
        self.youtube_db = config['youtube']['db']

    def retrieve_twitch_game_entries(self):
        """
        Retrieves TwitchScraper collections of the most viewed games from
        MongoDB.



        :return:
        """
        pass

    def aggregate_time(self, timestamps):
        """
        Returns the combined viewer hours.

        Adds the timestamps together accounting for the irregular intervals
        between them.  Timestamps that are further apart than fifteen minutes
        will result in inaccurate data as only the 15 minutes after the
        timestamp will be counted.

        :param timestamps: list, List of tuples where the first entry is the
            unix timestamp and the second entry is the viewer count.
        :return:
        """
        pass
