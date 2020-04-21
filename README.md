# A COVID-19 cough and symptoms classifier

## Setup

Make a virtual environment and source in:
```
python -m venv ./venv
source venv/bin/activate
```

Install the dependencies:
```
pip install -r requirements.txt
```

Create a new `.env` file and edit it with the correct **AWS Keys**:
```
cp .env.example .env
```

## Download the cough audio files

From the root of the project run:
```
python scripts/fetch_data.py
```

## Slice the .wav files into small cough clips

```
for coughfile in data/coughs/*.wav; do sox ${coughfile} ${coughfile%.wav}_cut.wav silence 1 0.1 0.3% 1 0.5 1% : newfile : restart; done
```