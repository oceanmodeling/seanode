Extract STOFS and related model data at station locations..

# Installation
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
git clone https://github.com:JackReevesEyre-NOAA/surge-stations.git
# or
git clone git@github.com:JackReevesEyre-NOAA/surge-stations.git 
```
### Install dependencies
```
cd surge-stations
python -m venv .venv
source /.venv/bin/activate
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

The package is not currently set up as a formal python project, so it needs to be added to your python search path manually (after cloning, above). This can be done by adding the following in whatever other project you want to use it from:
```
import os
import sys
sys.path.append(os.path.expanduser(<path_you_cloned_surge-stations_to>))
from seanode.api import get_surge_model_at_stations
```
Note that this assumes your working environment also contains at least the same dependencies as listed in `requirements.txt`.
