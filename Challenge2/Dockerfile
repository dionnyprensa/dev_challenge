FROM apache/airflow:2.4.1-python3.9

USER root
ARG AIRFLOW_USER_HOME=/opt/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
WORKDIR ${AIRFLOW_HOME}
ENV PYTHONPATH “${AIRFLOW_USER_HOME}/dags”:“$PYTHONPATH”
# COPY . ./
COPY --chown=airflow . ./

RUN apt-get update

USER airflow
RUN pip install pip --upgrade && pip install -r requirements.txt
RUN pip install protobuf==3.20.*

# USER root
# RUN chown -R airflow:root ${AIRFLOW_HOME}
# RUN chmod 755 ${AIRFLOW_HOME}