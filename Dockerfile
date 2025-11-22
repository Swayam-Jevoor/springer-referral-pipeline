
# Dockerfile for the referral pipeline
FROM python:3.11-slim
WORKDIR /app
COPY main.py /app/main.py
RUN pip install --no-cache-dir pandas python-dateutil pytz
# When running: mount host folder containing CSVs to /data_input and an output folder to /data_output
CMD ["python","/app/main.py"]
