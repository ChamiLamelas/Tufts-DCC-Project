#!/usr/bin/env bash

sudo apt-get update
sudo apt -y install python3.10
sudo apt install python3-pip
python3 -m pip install --upgrade pip
pip3 install pandas
python3 -m pip install --upgrade Pillow
pip3 install matplotlib
pip3 install scipy
pip3 install networkx

