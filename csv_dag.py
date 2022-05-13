# import sys, psycopg2
#
# conn = psycopg2.connect("dbname='falcon' user='postgres' host='localhost' password='postgres'")
# cur = conn.cursor()
# print('Connecting to Database')
# sql = "COPY (SELECT * FROM iris ) TO STDOUT WITH CSV DELIMITER ','"
# with open("/home/naveen/af2/psql.csv", "w") as file:
#     cur.copy_expert(sql, file)
#     cur.close()
# print('CSV File has been created')

import smtplib

# Python code to illustrate Sending mail from
# your Gmail account
import smtplib
s = smtplib.SMTP('smtp.gmail.com', 587)
s.starttls()
s.login("naveen@saturam.com", "Saturam$15")
message = "This is a test message"
s.sendmail("naveen@saturam.com", "naveenbharathic@gmail.com", message)
s.quit()


