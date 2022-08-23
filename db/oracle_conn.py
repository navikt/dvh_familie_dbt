# import cx_Oracle
# from os import getenv
# from dataverk_vault.api import set_secrets_as_envs



# def oracle_secrets():
#   set_secrets_as_envs()
#   return dict(
#     user=getenv("DB_USER"),
#     password=getenv("DB_PASSWORD"),
#     encoding="UTF-8",
#     nencoding="UTF-8"
#   )

# def connection(sql, dsn_tns):
#     """
#     lager en db-connection for querryen vi kjører
#     :param sql:
#     :return:
#     """
#     oracle_secrets = oracle_secrets()
#     #dsn_tns = cx_Oracle.makedsn(dsn_tns['host'], dsn_tns['port'], service_name = dsn_tns['service'])
#     try:
#         # establish a new connection
#         with cx_Oracle.connect(user = oracle_secrets.user,
#                             password = oracle_secrets.password,
#                             dsn = dsn_tns) as connection:
#             # create a cursor
#             with connection.cursor() as cursor:
#                 # execute the insert statement
#                 cursor.execute(sql)
#                 # commit the change
#                 connection.commit()
#     except cx_Oracle.Error as error:
#         print(error)

