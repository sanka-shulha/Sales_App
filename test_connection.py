from database import connect

conn = connect()

if conn:
    print("Підключення успішне")
    conn.close()
else:
    print("Помилка підключення")