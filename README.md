# Сбор данных и получение метрики из JSON-файла
Скрипт предназначен для импорта данных в БД PostgreSQL из предопределенного JSON-файла, для последующего расчета ряда метрик:

* Количество показов и уникальных пользователей за день в разрезе по платформам, в том числе по всем платформам суммарно;
* Количество за день уникальных авторов и уникального контента, показанного в ленте;
* Количество сессий, средняя глубина просмотра (по позиции фида) и средняя продолжительность пользовательской сессии в ленте за день.

---

## Установка и запуск
Для работы потребуется развернуть Docker-контейнер с PostgreSQL используя предоставленный `docker-compose.yml`:
```console
docker-compose up -d
```
В результате будет проинциализирована БД со следующей таблицей `json_data`:
```SQL
CREATE TABLE IF NOT EXISTS json_data (
  userid INT NOT NULL,
  platform text NOT NULL,
  durationms INT,
  position INT,
  timestamp timestamp NOT NULL,
  _group INT[],
  _user INT[],
  post INT[],
  movie INT[],
  user_photo INT[],
  group_photo INT[]);
```

Также необходимо установить требуемые зависимости:
```console
pip install -r requirements.txt
```

Запуск скрипта `main.py` не требует специальной настройки. Данные для подключения скрипта к БД помещены в `.env`.

В результате выполнения скрипта JSON данные будут вставлены в таблицу `json_data`:

| userid | platform | durationms | position | timestamp | \_group | \_user | post | movie | user\_photo | group\_photo |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 37686 | APP\_ANDROID | 1150 | 4 | 2019-09-12 19:47:06.000000 | null | null | null | null | null | null |
| 21783 | APP\_ANDROID | 3717 | 155 | 2019-09-12 15:02:31.000000 | {21055} | null | {39194} | {44250} | null | null |
| 50581 | APP\_ANDROID | 4208 | 42 | 2019-09-12 17:53:31.000000 | null | {50893} | null | null | {53655} | null |
| 27444 | DESKTOP\_WEB | 1787 | 41 | 2019-09-12 19:01:47.726000 | null | null | null | null | null | null |
| 56550 | APP\_ANDROID | 1254 | 34 | 2019-09-12 12:24:24.000000 | null | null | null | null | null | null |
| 39935 | APP\_ANDROID | 3331 | 55 | 2019-09-12 05:27:40.000000 | null | {3805} | {45513} | null | {52597} | null |
| 50802 | APP\_ANDROID | 8143 | 40 | 2019-09-12 01:36:56.000000 | {45075} | null | {176} | null | null | {4724} |
| 15900 | APP\_ANDROID | 1381 | 34 | 2019-09-12 05:27:02.000000 | null | {31900} | null | null | {3698} | null |
| 50581 | APP\_ANDROID | 45960 | 31 | 2019-09-12 18:50:46.000000 | null | null | null | null | null | null |
| 50302 | APP\_ANDROID | 1288 | 391 | 2019-09-12 12:12:38.000000 | {32831} | null | {45704} | null | null | {50624} |

---

### Получение метрики
* Количество показов и уникальных пользователей за день в разрезе по платформам:
```sql
SELECT DISTINCT "platform" as Платформа
          ,DATE(timestamp) as Дата
          ,COUNT(*) as Показы
          ,COUNT(DISTINCT userid) as Уникальные_пользователи
FROM json_data
GROUP BY Платформа, Дата
ORDER BY Дата, Платформа
```
| Платформа | Дата | Показы | Уникальные\_пользователи |
| :--- | :--- | :--- | :--- |
| APP\_ANDROID | 2019-09-11 | 834 | 9 |
| APP\_IOS | 2019-09-11 | 128 | 1 |
| DESKTOP\_WEB | 2019-09-11 | 86 | 1 |
| MOBILE\_WEB | 2019-09-11 | 16 | 1 |
| APP\_ANDROID | 2019-09-12 | 19037 | 114 |
| APP\_IOS | 2019-09-12 | 2342 | 26 |
| APP\_WINPHONE | 2019-09-12 | 247 | 1 |
| DESKTOP\_WEB | 2019-09-12 | 10266 | 54 |
| MOBILE\_WEB | 2019-09-12 | 1362 | 16 |


* Количество показов и уникальных пользователей за день по всем платформам:
```sql
SELECT DISTINCT DATE(timestamp) as Дата
       ,COUNT(*) as Показы_по_всем_платформам
       ,COUNT(DISTINCT userid) as Уникальные_пользователи
FROM json_data
GROUP BY Дата
```
| Дата | Показы\_по\_всем\_платформам | Уникальные\_пользователи |
| :--- | :--- | :--- |
| 2019-09-11 | 1064 | 12 |
| 2019-09-12 | 33254 | 199 |

* Количество за день уникальных авторов и уникального контента, показанного в ленте:
```sql
SELECT DATE(timestamp) as Дата,
         COUNT(DISTINCT _user) as Уник_Авторы_Польз
        ,COUNT(DISTINCT _group) as Уник_Авторы_Группы
        ,COUNT(DISTINCT _post) as Уник_Посты
        ,COUNT(DISTINCT _movie) as Уник_Видео
        ,COUNT(DISTINCT user_p) as Уник_Польз_Фото
        ,COUNT(DISTINCT group_p) as Уник_Груп_Фото
FROM json_data jd
LEFT JOIN LATERAL unnest("post") _post ON true
LEFT JOIN LATERAL unnest("movie") _movie ON true
LEFT JOIN LATERAL unnest("user_photo") user_p ON true
LEFT JOIN LATERAL unnest("group_photo") group_p ON true
GROUP BY Дата
```
| Дата | Уник\_Авторы\_Польз | Уник\_Авторы\_Группы | Уник\_Посты | Уник\_Видео | Уник\_Польз\_Фото | Уник\_Груп\_Фото |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2019-09-11 | 295 | 104 | 427 | 178 | 491 | 542 |
| 2019-09-12 | 9260 | 1669 | 11300 | 3629 | 19443 | 10632 |

* Количество сессий за день:
```sql
SELECT DATE(timestamp) as Дата
        ,COUNT(*) as Сессии FROM json_data
GROUP BY Дата
```
| Дата | Сессии |
| :--- | :--- |
| 2019-09-11 | 1064 |
| 2019-09-12 | 33254 |

* Средняя глубина просмотра (по позиции фида):
```sql
CREATE EXTENSION IF NOT EXISTS tablefunc;
CREATE TEMP TABLE IF NOT EXISTS tbl AS

select DATE(timestamp) as Дата
        ,position as Позиция_фида
        ,userid as Пользователь
        ,COUNT(*) as Количество_просмотров
from json_data
where position is not null
group by Дата, Пользователь, Позиция_фида
order by Дата, Позиция_фида, Пользователь

SELECT *
FROM crosstab(
         $$SELECT Позиция_фида
       , Дата
         , ROUND(SUM(Количество_просмотров) / COUNT(Пользователь), 2) as Результат
FROM tbl
GROUP BY Дата, Позиция_фида
        ORDER BY Позиция_фида, Дата$$
     )
AS cst("Позиция_фида" integer, "2019-09-11" numeric, "2019-09-12" numeric)
```
| Позиция\_фида | 2019-09-11 | 2019-09-12 |
|:--------------|:-----------|:-----------|
| 1             | 2.27       | 4.95       |
| 2             | 1.44       | 4.16       |
| 3             | 1.22       | 3.72       |
| 4             | 1.22       | 3.61       |
| ...           | ...        | ...        |
| 616           | 1          | null       |

* Средняя продолжительность пользовательской сессии в ленте за день:
```sql
SELECT DISTINCT DATE(timestamp) as Дата
        ,ROUND(SUM(durationms)/COUNT(*),0) as Сред_Сессия_Ms
    FROM json_data
    GROUP BY Дата
```
| Дата | Сред\_Сессия\_ms |
| :--- | :--- |
| 2019-09-11 | 24786 |
| 2019-09-12 | 27538 |
