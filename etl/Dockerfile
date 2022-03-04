# set base image 
FROM python:3.8-buster

# set the working directory in the container
WORKDIR /etl

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY data/ data
COPY src_etl/ src_etl

# command to run on container start
CMD ["python3","./src_etl/etl.py"] 

