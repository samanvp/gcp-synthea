# To build a new docker image, run the following from the root source dir:
# $ docker build . -f Dockerfile -t $IMAGE_NAME

FROM google/cloud-sdk:latest

#Install git
RUN apt-get update
RUN apt-get install -y git

RUN mkdir -p /opt/git/synthea
#Set working directory
WORKDIR /opt/git/synthea

RUN git clone https://github.com/synthetichealth/synthea.git /opt/git/synthea/
RUN /opt/git/synthea/gradlew build
#RUN /opt/git/synthea/gradlew check test
