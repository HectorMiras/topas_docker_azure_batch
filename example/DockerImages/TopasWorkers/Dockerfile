# Use Debian 10 (Buster) as the base image
FROM ubuntu:20.04 

# Set non-interactive mode for apt (to avoid prompts)
ENV DEBIAN_FRONTEND=noninteractive

# 1) Install Data Files:
WORKDIR /G4Data
RUN apt update && apt install -y wget tar
RUN wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4NDL.4.6.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4EMLOW.7.13.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4PhotonEvaporation.5.7.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4RadioactiveDecay.5.6.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4PARTICLEXS.3.1.1.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4SAIDDATA.2.0.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4ABLA.3.1.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4INCL.1.0.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4PII.1.3.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4ENSDFSTATE.2.3.tar.gz && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4RealSurface.2.2.tar.gz  && \
    wget -4 https://geant4-data.web.cern.ch/geant4-data/datasets/G4TENDL.1.3.2.tar.gz && \
    tar -zxf G4NDL.4.6.tar.gz && \
    tar -zxf G4EMLOW.7.13.tar.gz && \
    tar -zxf G4PhotonEvaporation.5.7.tar.gz && \
    tar -zxf G4RadioactiveDecay.5.6.tar.gz && \
    tar -zxf G4PARTICLEXS.3.1.1.tar.gz && \
    tar -zxf G4SAIDDATA.2.0.tar.gz && \
    tar -zxf G4ABLA.3.1.tar.gz && \
    tar -xzf G4INCL.1.0.tar.gz && \
    tar -zxf G4PII.1.3.tar.gz && \
    tar -zxf G4ENSDFSTATE.2.3.tar.gz && \
    tar -zxf G4RealSurface.2.2.tar.gz && \
    tar -zxf G4TENDL.1.3.2.tar.gz && \
    rm *.tar.gz # Delete all tar.gz files after extraction

# 2) Set up the environment:
ENV TOPAS_G4_DATA_DIR /G4Data

# 3) Pre-Requisites:
RUN apt update && apt install -y \
    libexpat1-dev \
    libgl1-mesa-dev \
    libglu1-mesa-dev \
    libxt-dev \
    xorg-dev \
    build-essential \
    libharfbuzz-dev \
    cmake \
    bash \
    unzip \
    nano \
    # Additional packages for Python3
    python3 \
    python3-pip \
    # install git
    git && \
    rm -rf /var/lib/apt/lists/*
    

# 4) Install TOPAS:
# Assuming the TOPAS files are available publicly. You might need to adjust this for your use.
# Set working directory
WORKDIR /tmp
COPY . .
RUN cat topas_3_9_debian9.tar.gz.part_* > topas_3_9_debian9.tar.gz && \
    tar -zxvf topas_3_9_debian9.tar.gz && \
    mv topas /topas && \
    rm -rf ./*


# 5) To add User Extensions (assuming the user does this part manually once in the container):
# Copy the topas_extensions directory to the container
COPY topas_extensions /topas_extensions
# Recompile with extensions
WORKDIR /topas
RUN unzip Geant4Headers.zip && \
    cmake -DTOPAS_EXTENSIONS_DIR=/topas_extensions && \
    make


# Entry command, here we simply start a bash shell so you can interact with the container
WORKDIR /topas/mytopassimulations
CMD ["/bin/bash"]
