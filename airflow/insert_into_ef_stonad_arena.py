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
    host = getenv("DBT_ORCL_HOST"),
    service = getenv("DBT_ORCL_SERVICE"),
    encoding="UTF-8",
    nencoding="UTF-8"
  )

oracle_secrets = oracle_secrets()
dsn_tns_HardCode = cx_Oracle.makedsn(oracle_secrets['host'], 1521, service_name = oracle_secrets['service'])

#user_proxy = str(oracle_secrets['user'])+"[dvh_fam_ef]"

# def connection(sql):
#     """
#     lager en db-connection for querryen vi kjører
#     :param sql:
#     :return:
#     """
#     #dsn_tns = cx_Oracle.makedsn(dsn_tns['host'], dsn_tns['port'], service_name = dsn_tns['service'])
#     dsn_tns_HardCode = cx_Oracle.makedsn(oracle_secrets['host'], 1521, service_name = oracle_secrets['service'])
#     try:
#         # establish a new connection
#         with cx_Oracle.connect(user = oracle_secrets['user'],
#                             password = oracle_secrets['password'],
#                             dsn = dsn_tns_HardCode) as connection:
#             # create a cursor
#             with connection.cursor() as cursor:
#                 # execute the insert statement
#                 cursor.execute(sql)
#                 # commit the change
#                 connection.commit()
#     except cx_Oracle.Error as error:
#         print(error)


def oracle_conn():
    connection = None
    try:
        connection = cx_Oracle.connect(user = oracle_secrets['user'], password = oracle_secrets['password'], dsn = dsn_tns_HardCode)

        # show the version of the Oracle Database
        print(connection.version)
        return connection
    except cx_Oracle.Error as error:
        print(error)
    finally:
        # release the connection
        if connection:
            connection.close()


def get_periode():
    """
    henter periode for the tidligere måneden eksample--> i dag er 19.04.2022, metoden vil kalkulerer periode aarMaaned eks) '202203'
    :param periode:
    :return: periode
    """
    today = datetime.date.today() # dato for idag 2022-04-19
    first = today.replace(day=1) # dato for første dag i måneden 2022-04-01
    lastMonth = first - datetime.timedelta(days=1) # dato for siste dag i tidligere måneden

    return lastMonth.strftime("%Y%m") # henter bare aar og maaned


# def give_grant():
#     sql = ('grant read on dvh_fam_ef.ef_stonad_arena_final to DVH_FAM_AIRFLOW')
#     connection(sql)

# def give_more_grants():
#     sql = ('grant insert, delete, select, update, read on dvh_fam_ef.fam_ef_stonad_arena to DVH_FAM_AIRFLOW')
#     connection(sql)

def send_context(conn):
    sql = ('''
        begin
            dbms_application_info.set_client_info( client_info => 'Klient_info Familie-Airflow');
            dbms_application_info.set_module( module_name => 'Kjører Team-familie Airflow applikasjon'
                                            , action_name => 'delete/insert into dvh_fam_ef.fam_ef_stonad_arena' );
        end;
    ''')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()

# def delete_data(periode):
#     """
#     sletter data fra fam_ef_stonad_arena med periode som kriteriea.
#     :param periode:
#     :return:
#     """
#     sql = ('delete from dvh_fam_ef.fam_ef_stonad_arena where periode =: periode')
#     connection(sql)

def delete_data(conn):
    """
    sletter data fra fam_ef_stonad_arena med periode som kriteriea.
    :param periode:
    :return:
    """
    sql = ('delete from dvh_fam_ef.fam_ef_stonad_arena where periode = 202207')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()

def insert_data(conn):
    """
    insert data fra ef_stonad_arena_final (view laget med dbt) into fam_ef_stonad_arena
    :param:
    :return:
    """
    sql = ('''
            INSERT INTO dvh_fam_ef.fam_ef_stonad_arena (FK_PERSON1,FK_DIM_PERSON,PERIODE,ALDER,KOMMUNE_NR,BYDEL_NR,KJONN_KODE,MAALGRUPPE_KODE
            ,MAALGRUPPE_NAVN,STATSBORGERSKAP,FODELAND,SIVILSTATUS_KODE,ANTBLAV,ANTBHOY,BARN_UNDER_18_ANTALL,INNTEKT_SISTE_BERAAR,INNTEKT_3_SISTE_BERAAR
            ,UTDSTONAD,TSOTILBARN,TSOLMIDLER,TSOBOUTG,TSODAGREIS,TSOREISOBL,TSOFLYTT,TSOREISAKT,TSOREISARB,TSOTILFAM,YBARN
            ,ANTBARN,ANTBU1,ANTBU3,ANTBU8,ANTBU10,ANTBU18,KILDESYSTEM,LASTET_DATO,OPPDATERT_DATO,FK_DIM_GEOGRAFI)
            SELECT FK_PERSON1,FK_DIM_PERSON,PERIODE,ALDER,KOMMUNE_NR,BYDEL_NR,KJONN_KODE,MAALGRUPPE_KODE
            ,MAALGRUPPE_NAVN,STATSBORGERSKAP,FODELAND,SIVILSTATUS_KODE,ANTBLAV,ANTBHOY,BARN_UNDER_18_ANTALL,INNTEKT_SISTE_BERAAR,INNTEKT_3_SISTE_BERAAR
            ,UTDSTONAD,TSOTILBARN,TSOLMIDLER,TSOBOUTG,TSODAGREIS,TSOREISOBL,TSOFLYTT,TSOREISAKT,TSOREISARB,TSOTILFAM,YBARN
            ,ANTBARN,ANTBU1,ANTBU3,ANTBU8,ANTBU10,ANTBU18,KILDESYSTEM,LASTET_DATO,OPPDATERT_DATO,FK_DIM_GEOGRAFI
            FROM dvh_fam_ef.ef_stonad_arena_final
        ''')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()

if __name__ == '__main__':
    #periode = get_periode()
    conn = oracle_conn()
    send_context(conn)
    delete_data(conn)
    #delete_data(periode)
    insert_data(conn)




