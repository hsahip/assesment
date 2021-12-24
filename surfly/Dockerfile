FROM python:latest
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY create_table_and_populate_data.py create_table_and_populate_data.py
COPY generate_kpi.py generate_kpi.py
CMD ["python", "-u", "create_table_and_populate_data.py"]
CMD ["python", "-u", "generate_kpi.py"]