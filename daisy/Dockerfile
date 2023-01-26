FROM iqtlabs/edgetech-core
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt
COPY ./dAISy_pub_sub.py .
ENTRYPOINT ["python3", "-u", "dAISy_pub_sub.py"]

## docker run -it --network host --device /dev/ttyACM0:/dev/serial0 <container name>
