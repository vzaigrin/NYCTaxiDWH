# DWH for NYC Taxi records
Домашнее задание по построению хранилища данных по курсу [Data Engineer](https://otus.ru/lessons/data-engineer/) от Otus.

## Источник данных
В качестве источника данных взяты записи о поездках "жёлтого" такси в Нью Йорке.
Данные находятся в открытом доступе [здесь](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Каждый файл представляет собой записи о поездках за месяц.
Описание данных находится [здесь](https://www1.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf) и [здесь](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
В записях используются идентификаторы зон посадок и высадок. Данные о зонах находятся [здесь](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv).

## Моделирование
Для моделирование DWH используется метод [Anchor Modeling](http://www.anchormodeling.com)

Основная сущность - поездки. Дополнительные сущности - зоны (районы) Нью Йорка и типы платежей.

### Stage
Данные о зонах и типах платежей фактически являются справочниками и могут быть сразу созданы в детальном слое. Но так как они могут измениться, они подаются в систему в виде файлов и проходят все этапы обработки.
Данные о поездках поступают в виде файлов с записями за месяц. Файлы располагаются в папке inbox. После обработки они перекладываются в папку archive.

Входящие файлы без изменений попадают в область Stage.
Из файлов удаляется заголовок, к каждой записи добавляется название источника ("TLC"), имя файла и время загрузки файла:
- Данные о типах платежей поступают в таблицу STG_PAYMENT_TYPE.
- Данные о зонах - в таблицу STG_ZONES.
- Данные о поездках - в таблицу STG_TRIP.

### ODS
Из области Stage данные перегружаются в область ODS с очисткой: удаляются дубликаты, записи с неправильными номерами зон, поездками с времен посадки позже времени высадки, нулевой дистанцией и отрицательной оплатой и колонки, не представляющие интерес для последующего анализа:
- Данные о типах платежей перегружаются из таблицы STG_PAYMENT_TYPE в ODS_PAYMENT_TYPE.
- Данные о зонах - из таблицы STG_ZONES в ODS_ZONES.
- Данные о поездках - из таблицы STG_TRIP в ODS_TRIP.

Для доступа к последним загруженным данным поверх таблиц ODS создаются представления (View):
- V_ODS_PAYMENT_TYPE поверх ODS_PAYMENT_TYPE
- V_ODS_ZONES поверх ODS_ZONES
- V_ODS_TRIP поверх ODS_TRIP

### DDS
В соответствии с концепцией Anchor Modeling в детальном слое создаются Anchor-таблицы с уникальными идентификаторами сущностей, таблицы с атрибутами сущностей и таблицы со связями между сущностями.

Эти таблицы наполняются из представлений V_ODS:
- V_ODS_PAYMENT_TYPE:
    - H_PAYMENT - уникальный идентификатор записи о типе платежа
    - S_PAYMENT_ID - идентификатор платежа
    - S_PAYMENT_TYPE - тип платежа
- V_ODS_ZONES:
    - H_LOCATION - уникальный идентификатор записи о зоне
    - S_LOCATION_ID - идентификатор зоны
    - S_LOCATION_BOROUGH - район
    - S_LOCATION_ZONE - название зоны
    - S_LOCATION_SERVICE_ZONE - название сервисной зоны
- V_ODS_TRIP:
    - H_TRIP - уникальный идентификатор записи о поездке
    - S_TRIP_PICKUP - время посадки
    - S_TRIP_DROPOFF - время высадки
    - S_TRIP_PASS_COUNT - кол-во пассажиров
    - S_TRIP_DISTANCE - дистанция поездки
    - S_TRIP_FARE - сумма по счётчику
    - S_TRIP_TIP - чаевые
    - S_TRIP_TOLLS - оплата дорог, мостов и т.п.
    - S_TRIP_TOTAL - полная сумма поездки

Для доступа к последним данным поверх таблиц DDS создаются представления (View):
- V_H_PAYMENT поверх H_PAYMENT
- v_S_PAYMENT_ID поверх S_PAYMENT_ID
- V_S_PAYMENT_TYPE поверх S_PAYMENT_TYPE
- V_H_LOCATION поверх H_LOCATION
- V_S_LOCATION_ID поверх S_LOCATION_ID
- V_S_LOCATION_BOROUGH поверх S_LOCATION_BOROUGH
- V_S_LOCATION_ZONE поверх S_LOCATION_ZONE
- V_S_LOCATION_SERVICE_ZONE поверх S_LOCATION_SERVICE_ZONE
- V_H_TRIP поверх H_TRIP
- V_S_TRIP_PICKUP поверх S_TRIP_PICKUP
- V_S_TRIP_DROPOFF поверх S_TRIP_DROPOFF
- V_S_TRIP_PASS_COUNT поверх S_TRIP_PASS_COUNT
- V_S_TRIP_DISTANCE поверх S_TRIP_DISTANCE
- V_S_TRIP_FARE поверх S_TRIP_FARE
- V_S_TRIP_TIP поверх S_TRIP_TIP
- V_S_TRIP_TOLLS поверх S_TRIP_TOLLS
- V_S_TRIP_TOTAL поверх S_TRIP_TOTAL

### Data Marts
Для анализа данных создано несколько витрин в виде представлений (View):
- V_DM_HOURLY_TS_ZONE - временной ряд (год, месяц, день, час) с суммарным количеством посадок в каждой зоне с агрерацией по часам
- V_DM_HOURLY_ZONE - суммарное количество посадок в каждой зоне в каждый час суток
- V_DM_DOW_ZONE - суммарное количество посадок в каждой зоне в каждый день недели
- V_DM_DOW_AVG_ZONE - средняя стоимость поездки в каждой зоне в каждый день недели
- V_DM_HOURLY_AVG_ZONE - средняя стоимость поездки в каждой зоне в каждый час суток

## Data Quality
Для проверки что все загруженные в слой ODS данные попали в слой DDS используются выражения из файла DQ.sql
Каждое выражение возвращает записи из слоя ODS, которые не попали в слой DDS.
