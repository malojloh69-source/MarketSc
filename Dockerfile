FROM python:3.12-slim
WORKDIR /app
COPY . /app
RUN useradd --create-home appuser && mkdir -p /app/data && chown -R appuser:appuser /app
USER appuser
ENV PYTHONUNBUFFERED=1
EXPOSE 3000
CMD ["python", "bot.py"]
