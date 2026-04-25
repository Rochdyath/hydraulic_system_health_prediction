FROM python:3.8-slim
WORKDIR /app
COPY app_requirements.txt .
RUN pip install -r app_requirements.txt
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]