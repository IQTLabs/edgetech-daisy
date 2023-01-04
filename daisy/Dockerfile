FROM iqtlabs/edgetech-core
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY dAISy.py dAISy.py
ENTRYPOINT ["python3", "dAISy.py"]

## docker run -it --network host --device /dev/ttyACM0:/dev/serial0 <container name>