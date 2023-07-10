FROM iqtlabs/edgetech-core

COPY pyproject.toml .
COPY poetry.lock .
RUN pip3 install poetry==1.5.1
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

COPY ./dAISy_pub_sub.py .

ENTRYPOINT ["python3", "dAISy_pub_sub.py"]

# TODO: Review
## docker run -it --network host --device /dev/ttyACM0:/dev/serial0 <container name>
