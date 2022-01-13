To set up a prefect-agent instance:

```
# install miniconda
curl -LO http://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -p ~/miniconda -b
rm Miniconda3-latest-Linux-x86_64.sh
echo "export PATH=${HOME}/miniconda/bin:${PATH}" >> ~/.bashrc
source ~/.bashrc
conda update -y conda
conda install -y -c conda-forge mamba
conda init bash
sudo su - $USER  # hack to apply conda settings

# set up conda env
mamba create -n prefect-exploration python=3.7
conda activate prefect-exploration

# MANUAL STEP: scp up requirements.txt

# install python deps
pip install -r requirements.txt

# auth prefect agent
prefect auth login --key KEY

# start prefect agent
prefect agent local start -l dev
```
