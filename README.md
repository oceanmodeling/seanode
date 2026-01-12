Extract STOFS and related model data at station locations..

# Installation
## Standard usage
For usage in your own python software, seanode can be installed with pip:
```
pip install git+ssh://git@github.com/oceanmodeling/seanode.git
```
Or add to a `requirements.txt` file:
```
<other packages>
...
seanode @  git+ssh://git@github.com/oceanmodeling/seanode.git
```
which can be used to create a virtual env:
```
pip install -r requirements.txt
```
## Development 
### Set up conda environment
This package has so far been developed and tested using `python 3.12`. If needed, use conda to get this:
```
# Download and set up conda:
wget "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
bash Miniforge3-Linux-x86_64.sh -b -p "${HOME}/conda"
source "${HOME}/conda/etc/profile.d/conda.sh"
source "${HOME}/conda/etc/profile.d/mamba.sh"
# Set up a new virtual environment:
mamba create --name=py312 python=3.12
mamba activate py312
```
### Clone repository
```
git clone https://github.com/oceanmodeling/seanode.git
# or
git clone git@github.com:oceanmodeling/seanode.git 
```
### Install dependencies
```
cd seanode
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
### Cleanup
When finished, both the `venv` and (if applicable) `conda` environments need to be deactivated:
```
deactivate
# if needed:
mamba deactivate
```

# Usage

The main function to use is `get_surge_model_at_stations(...)`:
```
from seanode.api import get_surge_model_at_stations
```
Examples of usage are given in `example_points_query.ipynb` and the scripts in directory `tests`.

There is also a command line interface tool that can be used to list available models and data. This has two usage patterns:

1. To list the available models:
```
python seanode_cli.py models
```

2. To list the data variables available for a particular model, and which dates they are available for:
```
python seanode_cli.py catalog <model_name>
```

You can define a `bash` (or other shell) alias to make these commands, and associated python environment, easier to work with. E.g., add the following to your `.bashrc` file:
```
alias seanode='<full_path_to_env_with_seanode_installed> <full_path_to/seanode_cli.py>'
# E.g.,
#alias seanode='/home/you/another_package/.venv/bin/python  /home/you/seanode/seanode/seanode_cli.py'
```
This can then be used like 
```
seanode models
# prints models
seanode catalog <model_name>
# prints data catalog for <model_name>
```


