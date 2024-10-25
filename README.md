# 7n_challenge

## Assumptions

### Dataset

The dataset data types detailed in the file FIR-0001_DataDictionary_fire-incidents.xlsx are outdated. To avoid copying every value detailed in the webpage by hand, pandas were used to load the CSV file and determine the data types this tool determined. However,  the documentation structure should be used to create the Postgres database.

 - Code used to load the dataset into the postgres database

```
import pandas

df = pandas.read_csv(r"C:\Users\user_name\Downloads\7n_challenge\wr8u-xric_version_5505.csv")

import psycopg2

conn_string = "postgresql://admin:admin@localhost:5433/recalls_db"

from sqlalchemy import create_engine

engine = create_engine(conn_string)

df.to_sql('raw.incidents', engine, if_exists='replace', index=False)
```

![img.png](AirflowAndPsql/Documentation/ReadmeImages/datasetCount.png)

 - Code used to load previous dates

```
alter table public."raw.incidents" add column _ingestion_date date;

update public."raw.incidents"
set _ingestion_date = '2024-10-24';
```

The same code was used to load the 2024-10-22 data, but the append strategy was used. 2024-10-23 manual download wasn't possible due to a webpage error.

```
df.to_sql('raw.incidents', engine, if_exists='append', index=False)
```

Ingestion information is added (file name, time, and other values can be added, too)

```
update public."raw.incidents"
set _ingestion_date = '2024-10-22'
where _ingestion_date is NULL;
```

![img.png](AirflowAndPsql/Documentation/ReadmeImages/datasetCountByDate.png)

 - Inspection of updated values

```
select yesterday.incident_number
from public."raw.incidents" yesterday
left join public."raw.incidents" today
	on today.incident_number = yesterday.incident_number
where md5(CAST((yesterday.*) AS text)) <> md5(CAST((today.*) AS text))
limit 1
```

A difference was found

![img.png](AirflowAndPsql/Documentation/ReadmeImages/datasetZipCodeIngestion.png)

This is not a difference; this step should be run over standardized tables. In this case, a fast fix will be implemented not to spend so much time.
Also, it is one of the motives why it is not recommended to use tools like pandas to load raw data, which implements datacasting that we don't control

```
update public."raw.incidents"
set zipcode = replace(zipcode, '.0', '')
```

It was also done for columns station_area and box.

After the fix and ignoring the data ingestion date columns, there is no row updated

<details>
<summary>See code</summary>

```
with yesterday as (
	select 
		"point",
		"neighborhood_district",
		"number_of_floors_with_significant_damage",
		"id",
		"zipcode",
		"data_as_of",
		"close_dttm",
		"incident_number",
		"arrival_dttm",
		"alarm_dttm",
		"incident_date",
		--"data_loaded_at",
		"exposure_number",
		"address",
		"call_number",
		"city",
		"battalion",
		"station_area",
		"box",
		"suppression_units",
		"suppression_personnel",
		"ems_units",
		"ems_personnel",
		"other_units",
		"other_personnel",
		"first_unit_on_scene",
		"estimated_property_loss",
		"estimated_contents_loss",
		"fire_fatalities",
		"fire_injuries",
		"civilian_fatalities",
		"civilian_injuries",
		"number_of_alarms",
		"primary_situation",
		"mutual_aid",
		"action_taken_primary",
		"action_taken_secondary",
		"action_taken_other",
		"detector_alerted_occupants",
		"property_use",
		"area_of_fire_origin",
		"ignition_cause",
		"ignition_factor_primary",
		"ignition_factor_secondary",
		"heat_source",
		"item_first_ignited",
		"human_factors_associated_with_ignition",
		"structure_type",
		"structure_status",
		"floor_of_fire_origin",
		"fire_spread",
		"no_flame_spread",
		"number_of_floors_with_minimum_damage",
		"number_of_floors_with_heavy_damage",
		"number_of_floors_with_extreme_damage",
		"detectors_present",
		"detector_type",
		"detector_operation",
		"detector_effectiveness",
		"detector_failure_reason",
		"automatic_extinguishing_system_present",
		"automatic_extinguishing_sytem_type",
		"automatic_extinguishing_sytem_perfomance",
		"automatic_extinguishing_sytem_failure_reason",
		"number_of_sprinkler_heads_operating",
		"supervisor_district"--,
		--"_ingestion_date"
	from public."raw.incidents"
	where _ingestion_date = '2024-10-22'
)
, today as (
	select 
		"point",
		"neighborhood_district",
		"number_of_floors_with_significant_damage",
		"id",
		"zipcode",
		"data_as_of",
		"close_dttm",
		"incident_number",
		"arrival_dttm",
		"alarm_dttm",
		"incident_date",
		--"data_loaded_at",
		"exposure_number",
		"address",
		"call_number",
		"city",
		"battalion",
		"station_area",
		"box",
		"suppression_units",
		"suppression_personnel",
		"ems_units",
		"ems_personnel",
		"other_units",
		"other_personnel",
		"first_unit_on_scene",
		"estimated_property_loss",
		"estimated_contents_loss",
		"fire_fatalities",
		"fire_injuries",
		"civilian_fatalities",
		"civilian_injuries",
		"number_of_alarms",
		"primary_situation",
		"mutual_aid",
		"action_taken_primary",
		"action_taken_secondary",
		"action_taken_other",
		"detector_alerted_occupants",
		"property_use",
		"area_of_fire_origin",
		"ignition_cause",
		"ignition_factor_primary",
		"ignition_factor_secondary",
		"heat_source",
		"item_first_ignited",
		"human_factors_associated_with_ignition",
		"structure_type",
		"structure_status",
		"floor_of_fire_origin",
		"fire_spread",
		"no_flame_spread",
		"number_of_floors_with_minimum_damage",
		"number_of_floors_with_heavy_damage",
		"number_of_floors_with_extreme_damage",
		"detectors_present",
		"detector_type",
		"detector_operation",
		"detector_effectiveness",
		"detector_failure_reason",
		"automatic_extinguishing_system_present",
		"automatic_extinguishing_sytem_type",
		"automatic_extinguishing_sytem_perfomance",
		"automatic_extinguishing_sytem_failure_reason",
		"number_of_sprinkler_heads_operating",
		"supervisor_district"--,
		--"_ingestion_date"
	from public."raw.incidents"
	where _ingestion_date = '2024-10-24'
)
select yesterday.incident_number
from yesterday
left join today
	on today.incident_number = yesterday.incident_number
where md5(CAST((yesterday.*) AS text)) <> md5(CAST((today.*) AS text))
limit 1
```
</details>

This implies that we only need to add the data of the file date in each daily ingestion.

### Ingestion process

 - Pending

Verify if the data from before "yesterday" could be present in a new file but not in the previous files that are supposed to be already loaded.

For instance:
    
    If on 2024-10-25 we are loading he 2024-10-24 data and there is a new value but from 2024-01-01 will not be loaded

In this case, this strategy does not work, and a complete revision of the file should be executed. Thus, the approach would be incremental with deduplication or another

### Dimensions

Following the same feeding strategy, a sub-set of columns could be selected  to create the dimensions, having a different primary related to the chosen dimension.

Also, tables containing pre-calculated aggregations could be updated every day. For instance, the number of incidences by date:

```
SELECT '{ds}'::date as incident_date, count(*) as quantity
FROM public."raw.incidents"
WHERE incident_date::date = {ds}::date
```

This step should be executed over a standardized table with incident_date cast to date
