# To build a new docker image, run the following from the root source dir:
# $ docker build . -f Dockerfile -t $IMAGE_NAME

FROM google/cloud-sdk:latest

#Install required tools
RUN apt-get update
RUN apt-get install -y git wget

RUN mkdir -p /opt/git/synthea
#Set working directory
WORKDIR /opt/git/synthea

RUN git clone https://github.com/synthetichealth/synthea.git /opt/git/synthea/
#RUN /opt/git/synthea/gradlew build check test

# Download the latest binary distribution.
# Please refer to the following page to find the latest release:
# github.com/synthetichealth/synthea/releases
RUN wget github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
RUN java -jar synthea-with-dependencies.jar -h

COPY docker/run_synthea.sh /opt/git/synthea
RUN chmod +x /opt/git/synthea/run_synthea.sh
ENTRYPOINT ["/opt/git/synthea/run_synthea.sh"]
