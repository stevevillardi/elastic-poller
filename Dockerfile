FROM python:3.12

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY edwin_elastic_poller/ edwin_elastic_poller/
RUN pip3 install --no-cache-dir "setuptools>=61" \
    && pip3 install --no-cache-dir --no-build-isolation .

ENV BOOKMARK_PATH=/data/

CMD ["python3", "-u", "-m", "edwin_elastic_poller"]
