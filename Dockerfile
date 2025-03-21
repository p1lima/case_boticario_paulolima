FROM python:3.9

WORKDIR /app
COPY . /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "read_files_save_BQ.py"]