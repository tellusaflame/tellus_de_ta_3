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
  feed_position INT,
  ts timestamp NOT NULL,
  owners_group INT[],
  owners_user INT[],
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

| userid | platform | durationms | feed\_position | ts | owners\_group | owners\_user | post | movie | user\_photo | group\_photo |
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
select distinct "platform" as platform
          ,date(ts) as date
          ,count(*) as uniq_shows
          ,count(distinct userid) as uniq_users
from json_data
group by platform, date
order by date, platform
```
| platform | date | uniq\_shows | uniq\_users |
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
select distinct date(ts) as date
       ,count(*) as shows_all_platforms
       ,count(distinct userid) as uniq_users
from json_data
group by date
```
| date | shows\_all\_platforms | uniq\_users |
| :--- | :--- | :--- |
| 2019-09-11 | 1064 | 12 |
| 2019-09-12 | 33254 | 199 |

* Количество за день уникальных авторов и уникального контента, показанного в ленте:
```sql
select date(ts) as date
        ,count(distinct owners_user) as uniq_author_user
        ,count(distinct owners_group) as uniq_author_group
        ,count(distinct _post) as uniq_posts
        ,count(distinct _movie) as uniq_video
        ,count(distinct user_p) as uniq_user_photo
        ,count(distinct group_p) as uniq_group_photo
from json_data jd
left join lateral unnest("post") _post on true
left join lateral unnest("movie") _movie on true
left join lateral unnest("user_photo") user_p on true
left join lateral unnest("group_photo") group_p on true
group by date
```
| date | uniq\_author\_user | uniq\_author\_group | uniq\_posts | uniq\_video | uniq\_user\_photo | uniq\_group\_photo |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2019-09-11 | 295 | 104 | 427 | 178 | 491 | 542 |
| 2019-09-12 | 9260 | 1669 | 11300 | 3629 | 19443 | 10632 |

* Количество сессий за день:
```sql
select date(ts) as date
        ,count(*) as sessions from json_data
group by date
```
| date | sessions |
| :--- | :--- |
| 2019-09-11 | 1064 |
| 2019-09-12 | 33254 |

* Средняя глубина просмотра (по позиции фида):
```sql
create extension if not exists tablefunc;
create temp table if not exists tbl as

select date(ts) as date
        ,feed_position as feed_position
        ,userid as users
        ,count(*) as cnt_views
from json_data
where feed_position is not null
group by date, users, feed_position
order by date, feed_position, users

select *
from crosstab(
         $$select feed_position
       , date
         , round(sum(cnt_views) / count(users), 2) as result
from tbl
group by date, feed_position
        order by feed_position, date$$
     )
as cst("feed_position" integer, "2019-09-11" numeric, "2019-09-12" numeric)
```
| feed\_position | 2019-09-11 | 2019-09-12 |
| :--- | :--- | :--- |
| 1 | 2.27 | 4.95 |
| 2 | 1.44 | 4.16 |
| 3 | 1.22 | 3.72 |
| 4 | 1.22 | 3.61 |
| 5 | 1.22 | 3.26 |
| 6 | 1.63 | 3.22 |
| 7 | 1.57 | 3.28 |
| 8 | 1.5 | 3.26 |
| 9 | 1.43 | 3.49 |
| 10 | 2.14 | 3.64 |

* Средняя продолжительность пользовательской сессии в ленте за день:
```sql
select distinct date(ts) as date
        ,round(sum(durationms)/count(*),0) as avg_session_ms
    from json_data
    group by date
```
| date | avg\_session\_ms |
| :--- | :--- |
| 2019-09-11 | 24786 |
| 2019-09-12 | 27538 |
