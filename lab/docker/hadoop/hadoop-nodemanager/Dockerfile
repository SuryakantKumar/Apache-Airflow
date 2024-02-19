FROM hadoop-base:latest
LABEL maintainer="SuryakantKumar"

COPY start-nodemanager.sh start-nodemanager.sh

RUN chmod +x start-nodemanager.sh

EXPOSE 8042

CMD [ "./start-nodemanager.sh" ]