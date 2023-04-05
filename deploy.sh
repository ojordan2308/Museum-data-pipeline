if [ -d "venv" ]; then
    source ./venv/bin/activate
else
    python3 -m venv venv
    source ./venv/bin/activate
    pip3 install -r requirements.txt
nohup python3 main.py &