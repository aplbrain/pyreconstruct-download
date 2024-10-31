# pyreconstruct-download
Tiny Flask app for downloading a subvolume for annotation in PyReconstruct, deployed with Zappa

Dependency: [uv](https://github.com/astral-sh/uv) 

To install locally:
```
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.cargo/env
git clone https://github.com/aplbrain/pyreconstruct-download.git
cd pyreconstruct-download
uv venv
```

To run locally:
```
source .venv/bin/activate
uv run flask run
```

