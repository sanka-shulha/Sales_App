import os
from decimal import Decimal
from datetime import date
import psycopg2

import config


# НАЛАШТУВАННЯ #
DB_CONFIG = config.DB_CONFIG
EXPORT_FILE_PATH = config.EXPORT_FILE_PATH


# ДОПОМІЖНІ ФУНКЦІЇ ДЛЯ БД #
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_all(sql, params=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def fetch_one(sql, params=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


def execute_dml(sql, params=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            affected = cur.rowcount
        conn.commit()
        return affected
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ДОПОМІЖНІ ФУНКЦІЇ ДЛЯ ВИВОДУ ТА ЗБЕРЕЖЕННЯ #
def _fmt(v):
    if isinstance(v, Decimal):
        return f"{v:.2f}"
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def rows_to_text(rows, header=None):
    lines = []
    if header:
        lines.append(header)
        lines.append("-" * len(header))
    if not rows:
        lines.append("Немає даних.")
        return "\n".join(lines)

    for r in rows:
        lines.append("(" + ", ".join(_fmt(x) for x in r) + ")")
    return "\n".join(lines)


def print_and_maybe_save(rows, header):
    text = rows_to_text(rows, header)
    print("\n" + text)

    ans = input("Зберегти результат у файл? (y/n): ").strip().lower()
    if ans in ("y", "yes", "1", "так", "т"):
        folder = os.path.dirname(EXPORT_FILE_PATH)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
        with open(EXPORT_FILE_PATH, "w", encoding="utf-8") as f:
            f.write(text)
        print("Результат збережено у файл.")
    return text


def input_int(msg):
    while True:
        s = input(msg).strip()
        if s.isdigit():
            return int(s)
        print("Введіть цілий ID.")


def input_decimal(msg):
    while True:
        s = input(msg).strip().replace(",", ".")
        try:
            val = Decimal(s)
            if val < 0:
                print("Сума не може бути від’ємною.")
                continue
            return val
        except Exception:
            print("Введіть число (наприклад 1200.50).")


def input_date(msg):
    while True:
        s = input(msg + " (YYYY-MM-DD): ").strip()
        parts = s.split("-")
        if len(parts) == 3 and all(p.isdigit() for p in parts) and len(parts[0]) == 4:
            return s
        print("Невірний формат. Приклад: 2026-02-15")


# ЗВІТИ (ЗАВДАННЯ 1) #

def r1_all_sales():
    sql = """
    SELECT s.id, s.sale_date, s.amount,
           sm.id, sm.full_name,
           c.id, c.full_name,
           s.note
    FROM sales s
    JOIN salesmen sm ON sm.id = s.salesman_id
    JOIN customers c ON c.id = s.customer_id
    ORDER BY s.id;
    """
    rows = fetch_all(sql)
    print_and_maybe_save(rows, "Усі угоди")


def r2_sales_of_salesman():
    sid = input_int("ID продавця: ")
    sql = """
    SELECT s.id, s.sale_date, s.amount,
           sm.full_name, c.full_name, s.note
    FROM sales s
    JOIN salesmen sm ON sm.id = s.salesman_id
    JOIN customers c ON c.id = s.customer_id
    WHERE s.salesman_id = %s
    ORDER BY s.id;
    """
    rows = fetch_all(sql, (sid,))
    print_and_maybe_save(rows, f"Угоди продавця ID={sid}")


def r3_max_sale():
    sql = """
    SELECT s.id, s.sale_date, s.amount,
           sm.full_name, c.full_name
    FROM sales s
    JOIN salesmen sm ON sm.id = s.salesman_id
    JOIN customers c ON c.id = s.customer_id
    ORDER BY s.amount DESC
    LIMIT 1;
    """
    row = fetch_one(sql)
    rows = [row] if row else []
    print_and_maybe_save(rows, "Максимальна за сумою угода")


def r4_min_sale():
    sql = """
    SELECT s.id, s.sale_date, s.amount,
           sm.full_name, c.full_name
    FROM sales s
    JOIN salesmen sm ON sm.id = s.salesman_id
    JOIN customers c ON c.id = s.customer_id
    ORDER BY s.amount ASC
    LIMIT 1;
    """
    row = fetch_one(sql)
    rows = [row] if row else []
    print_and_maybe_save(rows, "Мінімальна за сумою угода")


def r5_max_sale_for_salesman():
    sid = input_int("ID продавця: ")
    sql = """
    SELECT s.id, s.sale_date, s.amount,
           sm.full_name, c.full_name
    FROM sales s
    JOIN salesmen sm ON sm.id = s.salesman_id
    JOIN customers c ON c.id = s.customer_id
    WHERE s.salesman_id = %s
    ORDER BY s.amount DESC
    LIMIT 1;
    """
    row = fetch_one(sql, (sid,))
    rows = [row] if row else []
    print_and_maybe_save(rows, f"Максимальна сума угоди для продавця ID={sid}")


def r6_min_sale_for_salesman():
    sid = input_int("ID продавця: ")
    sql = """
    SELECT s.id, s.sale_date, s.amount,
           sm.full_name, c.full_name
    FROM sales s
    JOIN salesmen sm ON sm.id = s.salesman_id
    JOIN customers c ON c.id = s.customer_id
    WHERE s.salesman_id = %s
    ORDER BY s.amount ASC
    LIMIT 1;
    """
    row = fetch_one(sql, (sid,))
    rows = [row] if row else []
    print_and_maybe_save(rows, f"Мінімальна сума угоди для продавця ID={sid}")


def r7_max_sale_for_customer():
    cid = input_int("ID покупця: ")
    sql = """
    SELECT s.id, s.sale_date, s.amount,
           sm.full_name, c.full_name
    FROM sales s
    JOIN salesmen sm ON sm.id = s.salesman_id
    JOIN customers c ON c.id = s.customer_id
    WHERE s.customer_id = %s
    ORDER BY s.amount DESC
    LIMIT 1;
    """
    row = fetch_one(sql, (cid,))
    rows = [row] if row else []
    print_and_maybe_save(rows, f"Максимальна сума угоди для покупця ID={cid}")


def r8_min_sale_for_customer():
    cid = input_int("ID покупця: ")
    sql = """
    SELECT s.id, s.sale_date, s.amount,
           sm.full_name, c.full_name
    FROM sales s
    JOIN salesmen sm ON sm.id = s.salesman_id
    JOIN customers c ON c.id = s.customer_id
    WHERE s.customer_id = %s
    ORDER BY s.amount ASC
    LIMIT 1;
    """
    row = fetch_one(sql, (cid,))
    rows = [row] if row else []
    print_and_maybe_save(rows, f"Мінімальна сума угоди для покупця ID={cid}")


def r9_salesman_max_total():
    sql = """
    SELECT sm.id, sm.full_name, COALESCE(SUM(s.amount), 0) AS total_sales
    FROM salesmen sm
    LEFT JOIN sales s ON s.salesman_id = sm.id
    GROUP BY sm.id, sm.full_name
    ORDER BY total_sales DESC
    LIMIT 1;
    """
    row = fetch_one(sql)
    rows = [row] if row else []
    print_and_maybe_save(rows, "Продавець з максимальною сумою продажів")


def r10_salesman_min_total():
    sql = """
    SELECT sm.id, sm.full_name, COALESCE(SUM(s.amount), 0) AS total_sales
    FROM salesmen sm
    LEFT JOIN sales s ON s.salesman_id = sm.id
    GROUP BY sm.id, sm.full_name
    ORDER BY total_sales ASC
    LIMIT 1;
    """
    row = fetch_one(sql)
    rows = [row] if row else []
    print_and_maybe_save(rows, "Продавець з мінімальною сумою продажів")


def r11_customer_max_total():
    sql = """
    SELECT c.id, c.full_name, COALESCE(SUM(s.amount), 0) AS total_purchases
    FROM customers c
    LEFT JOIN sales s ON s.customer_id = c.id
    GROUP BY c.id, c.full_name
    ORDER BY total_purchases DESC
    LIMIT 1;
    """
    row = fetch_one(sql)
    rows = [row] if row else []
    print_and_maybe_save(rows, "Покупець з максимальною сумою покупок")


def r12_customer_avg():
    cid = input_int("ID покупця: ")
    sql = """
    SELECT c.id, c.full_name, AVG(s.amount) AS avg_purchase
    FROM customers c
    JOIN sales s ON s.customer_id = c.id
    WHERE c.id = %s
    GROUP BY c.id, c.full_name;
    """
    row = fetch_one(sql, (cid,))
    rows = [row] if row else []
    print_and_maybe_save(rows, f"Середня сума покупки для покупця ID={cid}")


def r13_salesman_avg():
    sid = input_int("ID продавця: ")
    sql = """
    SELECT sm.id, sm.full_name, AVG(s.amount) AS avg_sale
    FROM salesmen sm
    JOIN sales s ON s.salesman_id = sm.id
    WHERE sm.id = %s
    GROUP BY sm.id, sm.full_name;
    """
    row = fetch_one(sql, (sid,))
    rows = [row] if row else []
    print_and_maybe_save(rows, f"Середня сума покупки для продавця ID={sid}")


# CRUD (ЗАВДАННЯ 2) #

# CRUD SALESmen #
def salesmen_list():
    rows = fetch_all("SELECT id, full_name, phone, is_active FROM salesmen ORDER BY id;")
    print_and_maybe_save(rows, "Продавці")


def salesmen_add():
    name = input("ПІБ продавця: ").strip()
    phone = input("Телефон: ").strip()
    active_in = input("Активний? (y/n, Enter=y): ").strip().lower()
    is_active = True if active_in in ("", "y", "yes", "1", "так", "т") else False

    if not name:
        print("ПІБ не може бути порожнім.")
        return

    sql = """
    INSERT INTO salesmen (full_name, phone, is_active)
    VALUES (%s, %s, %s)
    RETURNING id;
    """
    new_id = fetch_one(sql, (name, phone or None, is_active))[0]
    print(f"Додано продавця. ID={new_id}")


def salesmen_update():
    sid = input_int("ID продавця для оновлення: ")
    row = fetch_one("SELECT full_name, phone, is_active FROM salesmen WHERE id=%s;", (sid,))
    if not row:
        print("Продавця з таким ID не знайдено.")
        return

    cur_name, cur_phone, cur_active = row

    name = input("Нове ПІБ (Enter = не змінювати): ").strip()
    phone = input("Новий телефон (Enter = не змінювати): ").strip()
    active_in = input("Активний? (y/n, Enter = не змінювати): ").strip().lower()

    new_name = name if name else cur_name
    new_phone = phone if phone else cur_phone

    if active_in == "":
        new_active = cur_active
    else:
        new_active = True if active_in in ("y", "yes", "1", "так", "т") else False

    affected = execute_dml(
        "UPDATE salesmen SET full_name=%s, phone=%s, is_active=%s WHERE id=%s;",
        (new_name, new_phone, new_active, sid)
    )
    print(f"Оновлено рядків: {affected}")


def salesmen_delete():
    sid = input_int("ID продавця для видалення: ")
    affected = execute_dml("DELETE FROM salesmen WHERE id=%s;", (sid,))
    print(f"Видалено рядків: {affected}")


# CRUD CUSTOMERS #
def customers_list():
    rows = fetch_all("SELECT id, full_name, phone, email, is_active FROM customers ORDER BY id;")
    print_and_maybe_save(rows, "Покупці")


def customers_add():
    name = input("ПІБ покупця: ").strip()
    phone = input("Телефон: ").strip()
    email = input("Email: ").strip()
    active_in = input("Активний? (y/n, Enter=y): ").strip().lower()
    is_active = True if active_in in ("", "y", "yes", "1", "так", "т") else False

    if not name:
        print("ПІБ не може бути порожнім.")
        return

    sql = """
    INSERT INTO customers (full_name, phone, email, is_active)
    VALUES (%s, %s, %s, %s)
    RETURNING id;
    """
    new_id = fetch_one(sql, (name, phone or None, email or None, is_active))[0]
    print(f"Додано покупця. ID={new_id}")


def customers_update():
    cid = input_int("ID покупця для оновлення: ")
    row = fetch_one("SELECT full_name, phone, email, is_active FROM customers WHERE id=%s;", (cid,))
    if not row:
        print("Покупця з таким ID не знайдено.")
        return

    cur_name, cur_phone, cur_email, cur_active = row

    name = input("Нове ПІБ (Enter = не змінювати): ").strip()
    phone = input("Новий телефон (Enter = не змінювати): ").strip()
    email = input("Новий email (Enter = не змінювати): ").strip()
    active_in = input("Активний? (y/n, Enter = не змінювати): ").strip().lower()

    new_name = name if name else cur_name
    new_phone = phone if phone else cur_phone
    new_email = email if email else cur_email

    if active_in == "":
        new_active = cur_active
    else:
        new_active = True if active_in in ("y", "yes", "1", "так", "т") else False

    affected = execute_dml(
        "UPDATE customers SET full_name=%s, phone=%s, email=%s, is_active=%s WHERE id=%s;",
        (new_name, new_phone, new_email, new_active, cid)
    )
    print(f"Оновлено рядків: {affected}")


def customers_delete():
    cid = input_int("ID покупця для видалення: ")
    affected = execute_dml("DELETE FROM customers WHERE id=%s;", (cid,))
    print(f"Видалено рядків: {affected}")


# CRUD SALES #
def sales_list():
    sql = """
    SELECT s.id, s.sale_date, s.amount, s.salesman_id, s.customer_id, s.note
    FROM sales s
    ORDER BY s.id;
    """
    rows = fetch_all(sql)
    print_and_maybe_save(rows, "Угоди")


def sales_add():
    print("Підказка: спочатку подивіться IDs продавців та покупців (CRUD -> список).")
    sid = input_int("ID продавця: ")
    cid = input_int("ID покупця: ")
    amount = input_decimal("Сума: ")
    sale_date = input_date("Дата угоди")
    note = input("Примітка: ").strip()

    sql = """
    INSERT INTO sales (salesman_id, customer_id, amount, sale_date, note)
    VALUES (%s, %s, %s, %s, %s)
    RETURNING id;
    """
    new_id = fetch_one(sql, (sid, cid, amount, sale_date, note or None))[0]
    print(f"Додано угоду. ID={new_id}")


def sales_update():
    sale_id = input_int("ID угоди для оновлення: ")
    row = fetch_one("SELECT salesman_id, customer_id, amount, sale_date, note FROM sales WHERE id=%s;", (sale_id,))
    if not row:
        print("Угоду з таким ID не знайдено.")
        return

    cur_sid, cur_cid, cur_amount, cur_date, cur_note = row

    sid_in = input("ID продавця (Enter = не змінювати): ").strip()
    cid_in = input("ID покупця (Enter = не змінювати): ").strip()
    amount_in = input("Сума (Enter = не змінювати): ").strip().replace(",", ".")
    date_in = input("Дата (YYYY-MM-DD, Enter = не змінювати): ").strip()
    note_in = input("Примітка (Enter = не змінювати): ").strip()

    new_sid = int(sid_in) if sid_in.isdigit() else cur_sid
    new_cid = int(cid_in) if cid_in.isdigit() else cur_cid
    new_amount = Decimal(amount_in) if amount_in else cur_amount
    new_date = date_in if date_in else cur_date
    new_note = note_in if note_in else cur_note

    affected = execute_dml(
        "UPDATE sales SET salesman_id=%s, customer_id=%s, amount=%s, sale_date=%s, note=%s WHERE id=%s;",
        (new_sid, new_cid, new_amount, new_date, new_note, sale_id)
    )
    print(f"Оновлено рядків: {affected}")


def sales_delete():
    sale_id = input_int("ID угоди для видалення: ")
    affected = execute_dml("DELETE FROM sales WHERE id=%s;", (sale_id,))
    print(f"Видалено рядків: {affected}")


# МЕНЮ #
def menu_reports():
    while True:
        print("\n=== REPORTS MENU (Task 1) ===")
        print("1.  Відображення усіх угод")
        print("2.  Відображення угод конкретного продавця")
        print("3.  Відображення максимальної за сумою угоди")
        print("4.  Відображення мінімальної за сумою угоди")
        print("5.  Відображення максимальної суми угоди для конкретного продавця")
        print("6.  Відображення мінімальної за сумою угоди для конкретного продавця")
        print("7.  Відображення максимальної за сумою угоди для конкретного покупця")
        print("8.  Відображення мінімальної за сумою угоди для конкретного покупця")
        print("9.  Відображення продавця з максимальною сумою продажів за всіма угодами")
        print("10. Відображення продавця з мінімальною сумою продажів за всіма угодами")
        print("11. Відображення покупця з максимальною сумою покупок за всіма угодами")
        print("12. Відображення середньої суми покупки для конкретного покупця")
        print("13. Відображення середньої суми покупки для конкретного продавця")
        print("0.  Назад")

        ch = input("Оберіть пункт: ").strip()
        try:
            if ch == "1":
                r1_all_sales()
            elif ch == "2":
                r2_sales_of_salesman()
            elif ch == "3":
                r3_max_sale()
            elif ch == "4":
                r4_min_sale()
            elif ch == "5":
                r5_max_sale_for_salesman()
            elif ch == "6":
                r6_min_sale_for_salesman()
            elif ch == "7":
                r7_max_sale_for_customer()
            elif ch == "8":
                r8_min_sale_for_customer()
            elif ch == "9":
                r9_salesman_max_total()
            elif ch == "10":
                r10_salesman_min_total()
            elif ch == "11":
                r11_customer_max_total()
            elif ch == "12":
                r12_customer_avg()
            elif ch == "13":
                r13_salesman_avg()
            elif ch == "0":
                break
            else:
                print("Невірний пункт.")
        except psycopg2.Error as e:
            print("Помилка PostgreSQL:", e)


def menu_crud():
    while True:
        print("\n=== CRUD MENU (Task 2) ===")
        print("1. Продавці: показати")
        print("2. Продавці: додати")
        print("3. Продавці: оновити (по ID)")
        print("4. Продавці: видалити (по ID)")
        print("5. Покупці: показати")
        print("6. Покупці: додати")
        print("7. Покупці: оновити (по ID)")
        print("8. Покупці: видалити (по ID)")
        print("9. Угоди: показати")
        print("10. Угоди: додати")
        print("11. Угоди: оновити (по ID)")
        print("12. Угоди: видалити (по ID)")
        print("0. Назад")

        ch = input("Оберіть пункт: ").strip()
        try:
            if ch == "1":
                salesmen_list()
            elif ch == "2":
                salesmen_add()
            elif ch == "3":
                salesmen_update()
            elif ch == "4":
                salesmen_delete()
            elif ch == "5":
                customers_list()
            elif ch == "6":
                customers_add()
            elif ch == "7":
                customers_update()
            elif ch == "8":
                customers_delete()
            elif ch == "9":
                sales_list()
            elif ch == "10":
                sales_add()
            elif ch == "11":
                sales_update()
            elif ch == "12":
                sales_delete()
            elif ch == "0":
                break
            else:
                print("Невірний пункт.")
        except psycopg2.Error as e:
            print("Помилка PostgreSQL:", e)


def menu_main():
    while True:
        print("\n=== ГОЛОВНЕ МЕНЮ ===")
        print("1. Звіти (Task 1 + Task 3)")
        print("2. Редагування даних (Task 2)")
        print("0. Вихід")

        ch = input("Оберіть пункт: ").strip()

        if ch == "1":
            menu_reports()
        elif ch == "2":
            menu_crud()
        elif ch == "0":
            break
        else:
            print("Невірний пункт.")


if __name__ == "__main__":
    menu_main()