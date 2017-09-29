from setuptools import setup, find_packages


setup(
    name='esportstracker',
    version='0.1',
    packages=find_packages(),
    entry_points={
        'console_scripts':[
            'twitchscraper = esportstracker.entrypoints:run_twitchscraper',
            'youtubescraper = esportstracker.entrypoints:run_youtubescraper',
            'aggregator = esportsracker.entrypoints:run_aggregator'
        ]
    },
    install_requires=[
        'pyyaml',
        'pymongo',
        'requests',
        'pytest',
        'ruamel.yaml',
        'psycopg2',
        'setuptools',
        'langid',
        'setproctitle',
        'pytz'
    ],
    author='Rowan Meara',
    author_email='rowanmeara@gmail.com',
    url='https://www.esportstracker.net'
)