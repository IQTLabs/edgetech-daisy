FROM iqtlabs/edgetech-core
COPY pyproject.toml .
RUN pip3 install poetry==1.1.5
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
COPY ./dAISy_pub_sub.py .
ENTRYPOINT ["python3", "dAISy_pub_sub.py"]

## docker run -it --network host --device /dev/ttyACM0:/dev/serial0 <container name>
