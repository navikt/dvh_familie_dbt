#from db.oracle_conn import connection as conn

import datetime
import cx_Oracle
from os import getenv
from dataverk_vault.api import set_secrets_as_envs

def oracle_secrets():
  set_secrets_as_envs()
  return dict(
    user=getenv("AIRFLOW_ORCL_USER"),
    password=getenv("AIRFLOW_ORCL_PASS"),
    encoding="UTF-8",
    nencoding="UTF-8"
  )

def connection(sql):
    """
    lager en db-connection for querryen vi kjører
    :param sql:
    :return:
    """
    oracle_secrets = oracle_secrets()
    #dsn_tns = cx_Oracle.makedsn(dsn_tns['host'], dsn_tns['port'], service_name = dsn_tns['service'])
    dsn_tns_HardCode = cx_Oracle.makedsn('dm07-scan.adeo.no', 1521, service_name = 'dwhr')
    try:
        # establish a new connection
        with cx_Oracle.connect(user = oracle_secrets.user,
                            password = oracle_secrets.password,
                            dsn = dsn_tns_HardCode) as connection:
            # create a cursor
            with connection.cursor() as cursor:
                # execute the insert statement
                cursor.execute(sql)
                # commit the change
                connection.commit()
    except cx_Oracle.Error as error:
        print(error)


# def get_periode():
#     """
#     henter periode for the tidligere måneden eksample--> i dag er 19.04.2022, metoden vil kalkulerer periode aarMaaned eks) '202203'
#     :param periode:
#     :return: periode
#     """
#     today = datetime.date.today() # dato for idag 2022-04-19
#     first = today.replace(day=1) # dato for første dag i måneden 2022-04-01
#     lastMonth = first - datetime.timedelta(days=1) # dato for siste dag i tidligere måneden

#     return lastMonth.strftime("%Y%m") # henter bare aar og maaned


def delete_data(periode):
    """
    sletter data fra fam_ef_stonad_arena med periode som kriteriea.
    :param periode:
    :return:
    """
    sql = ('delete from fam_ef_stonad_arena where periode =: periode')
    connection(sql)



def insert_data():
    """
    insert data fra ef_stonad_arena_final (view laget med dbt) into fam_ef_stonad_arena
    :param:
    :return:
    """
    sql = ('''
            INSERT INTO fam_ef_stonad_arena (FK_PERSON1,FK_DIM_PERSON,PERIODE,ALDER,KOMMUNE_NR,BYDEL_NR,KJONN_KODE,MAALGRUPPE_KODE
            ,MAALGRUPPE_NAVN,STATSBORGERSKAP,FODELAND,SIVILSTATUS_KODE,ANTBLAV,ANTBHOY,BARN_UNDER_18_ANTALL,INNTEKT_SISTE_BERAAR,INNTEKT_3_SISTE_BERAAR
            ,UTDSTONAD,TSOTILBARN,TSOLMIDLER,TSOBOUTG,TSODAGREIS,TSOREISOBL,TSOFLYTT,TSOREISAKT,TSOREISARB,TSOTILFAM,YBARN
            ,ANTBARN,ANTBU1,ANTBU3,ANTBU8,ANTBU10,ANTBU18,KILDESYSTEM,LASTET_DATO,OPPDATERT_DATO,FK_DIM_GEOGRAFI)
            SELECT FK_PERSON1,FK_DIM_PERSON,PERIODE,ALDER,KOMMUNE_NR,BYDEL_NR,KJONN_KODE,MAALGRUPPE_KODE
            ,MAALGRUPPE_NAVN,STATSBORGERSKAP,FODELAND,SIVILSTATUS_KODE,ANTBLAV,ANTBHOY,BARN_UNDER_18_ANTALL,INNTEKT_SISTE_BERAAR,INNTEKT_3_SISTE_BERAAR
            ,UTDSTONAD,TSOTILBARN,TSOLMIDLER,TSOBOUTG,TSODAGREIS,TSOREISOBL,TSOFLYTT,TSOREISAKT,TSOREISARB,TSOTILFAM,YBARN
            ,ANTBARN,ANTBU1,ANTBU3,ANTBU8,ANTBU10,ANTBU18,KILDESYSTEM,LASTET_DATO,OPPDATERT_DATO,FK_DIM_GEOGRAFI
            FROM ef_stonad_arena_final
        ''')
    connection(sql)

if __name__ == '__main__':
    periode = 202207 #get_periode()
    delete_data(periode)
    insert_data()




