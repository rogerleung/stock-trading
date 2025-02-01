# Trading-System

Trading-System is a folder to practice various trading strategy with python.

# Set up environment

```bash
python3.11 -m venv .venv 

Windows
.\.venv\Scripts\Activate.ps1

Linux or Mac
source .venv/bin/activate

After Activated
pip install -r requirements.txt
```

# Copy Binance API Credentials into config file
copy your credentials into the folowing document
```
config.py 
```

# build docker 

```bash
cd docker
docker-compose up --build
```

# run strategy and executioner

```bash
python .\producer.py
python .\executioner.py  
```

# run streamlit from main directory
```bash
streamlit run .\main.py
```

# Upcoming Features
1. Backtesting module and visualisation
2. Multiple strategy
3. Attach strategy to orders for analysis
4. Parameterize strategy for easy deployment and tuning

# Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

# License

[MIT](https://choosealicense.com/licenses/mit/)