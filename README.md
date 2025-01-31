# Trading-System

Trading-System is a folder to practice various trading strategy with python.

# set up environment

```bash
python3.11 -m venv .venv 

Windows
.\.venv\Scripts\Activate.ps1

Linux or Mac
source .venv/bin/activate

After Activated
pip install -r requirements.txt

# Copy Binance API Credentials into config file
config.py

# copy config and requirements to containers
copy .\config.py .\docker\kafka\ 
copy .\requirements.txt .\docker\

# build docker 
cd docker
docker-compose up --build

# run streamlit from main directory
streamlit run .\main.py


```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)