from setuptools import setup

APP = ['Main.py']
OPTIONS = {
    'argv_emulation': False,
    'resources': [
        'credentials.json',
        'config.json',
    ],
}

setup(
    app=APP,
    data_files=[],
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
)
