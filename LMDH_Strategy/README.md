# Introduction 
This project is a race tracking and strategy tool for WEC track engineers. It is entirely implemented in python, using streamlit for the interface.

## Getting Started
All of the following steps must be performed from the project root `~/LMDH_Strategy/`: 
1.	Installation required packages (we assume that `python3` and `pip3` are installed on the computer)
```bash
pip3 install -r requirements.txt
```
2.	Launch App
```bash
mkdir data/drivers
mkdir data/tyres
python3 -m streamlit run front/main.py
``` 
3.	Launch Tests
```bash
python3 -m unittest
python3 -m unittest -v # for the verbose version
``` 

### Contributor
- #### Fofana Cheick Tidiane
- #### Lacoudre Erwan