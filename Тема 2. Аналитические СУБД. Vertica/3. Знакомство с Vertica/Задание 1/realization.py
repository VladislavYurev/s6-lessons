import vertica_python

conn_info = {
    'host': 'vertica.tgcloudenv.ru', 
    'port': 5433,  # Порт должен быть числом, не строкой
    'user': 'stv2025032024',  # Убедитесь, что пользователь без пробелов
    'password': 'hNBHlENmWqyMI6s',
    'database': 'dwh',
    'autocommit': True
}

def try_select(conn_info=conn_info):
    # Рекомендуем использовать соединение вот так
    with vertica_python.connect(**conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS a1;") 
            res = cur.fetchall()  # Переместите это внутрь контекстного менеджера
    return res

# Вызовите функцию, чтобы получить результат
result = try_select()
print(result)