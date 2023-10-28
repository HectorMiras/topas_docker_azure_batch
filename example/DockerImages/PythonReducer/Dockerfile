FROM python:3

WORKDIR /usr/src/app

COPY . .

#COPY requirements.txt ./
RUN pip install numpy \
    		pandas \
    		matplotlib \
    		scipy \
    		azure-storage-blob \
    		azure-batch \
    		azure-core \
    		azure-common

# Set a default command to keep the container running with bash interaction
CMD ["bash"]
