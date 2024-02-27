FROM python:3.8
COPY app /app
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
WORKDIR /app
CMD python main.py --client dataset_one.csv --financial dataset_two.csv --countries United Kingdom Netherlands
