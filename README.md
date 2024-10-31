# pyreconstruct-download
Tiny Flask app for downloading a subvolume for annotation in PyReconstruct, deployed with Zappa

Dependency: [conda](https://docs.anaconda.com/miniconda/) 

To install locally:
```
git clone https://github.com/aplbrain/pyreconstruct-download.git
cd pyreconstruct-download
conda env create -f environment.yaml
```

To run locally:
```
conda activate pyreconstruct-download
flask run --debug
```

