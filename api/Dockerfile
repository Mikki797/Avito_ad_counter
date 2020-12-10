FROM python:3.7.9-slim

ADD api.py /api/
ADD api_types.py /api/
ADD database.py /api/
ADD main.py /api/

ADD requirements.txt /api/

WORKDIR /api/

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

EXPOSE 8000