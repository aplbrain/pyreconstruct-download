# pyreconstruct-download
Tiny Flask app for downloading a subvolume for annotation in PyReconstruct, deployed with Zappa

Dependency: Python 3.10

To install locally:
```
git clone https://github.com/aplbrain/pyreconstruct-download.git
cd pyreconstruct-download
python -m venv venv
```

To run locally:
```
source venv/bin/activate
flask run --debug
```

To deploy changes:
```
zappa update
```
