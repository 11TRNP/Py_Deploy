# start from python base image
FROM python:3.9

# change working directory
WORKDIR /BISMILLAH

# add requirementes file to image
COPY requirements.txt /BISMILLAH/requirements.txt

# install python libraries
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    --default-timeout=600 \
    --retries=10 \
    -r requirements.txt \
    -f https://download.pytorch.org/whl/cpu

# append project and date directories to PYTHONPATH \
ENV PYTHONPATH=/BISMILLAH

# add python code \
COPY . .

#specify default commands
CMD ["python", "main.py"]