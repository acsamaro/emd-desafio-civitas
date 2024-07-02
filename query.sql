WITH
-- Filtra dados válidos
filtered_valid_data AS (
  SELECT DISTINCT
    TO_HEX(camera_numero) AS camera_numero,
    TO_HEX(placa) AS placa,
    TO_HEX(empresa) AS empresa,
    TO_HEX(tipoveiculo) AS tipoveiculo,
    camera_latitude,
    camera_longitude,
    datahora,
    datahora_captura,
    velocidade
  FROM
    `rj-cetrio.desafio.readings_2024_06`
  WHERE
    datahora IS NOT NULL
    AND datahora_captura IS NOT NULL
    AND placa IS NOT NULL
    AND empresa IS NOT NULL
    AND tipoveiculo IS NOT NULL
    AND velocidade IS NOT NULL
    AND camera_numero IS NOT NULL
    AND camera_latitude IS NOT NULL
    AND camera_longitude IS NOT NULL
    AND velocidade > 0
    AND NOT (velocidade > 140 AND EXTRACT(HOUR FROM datahora) BETWEEN 6 AND 22)
    AND ST_WITHIN(
        ST_GEOGPOINT(camera_longitude, camera_latitude),
        ST_GEOGFROMTEXT('POLYGON((-43.795 -23.082, -43.105 -23.082, -43.105 -22.738, -43.795 -22.738, -43.795 -23.082))')
    )
),

-- Filtra placas que aparecem mais de uma vez
filtered_plates AS (
  SELECT
    placa
  FROM
    filtered_valid_data
  GROUP BY
    placa
  HAVING
    COUNT(*) > 1
),

-- Filtra dados válidos com placas que aparecem mais de uma vez
valid_data_with_multiple_plates AS (
  SELECT
    *
  FROM
    filtered_valid_data
  WHERE
    placa IN (SELECT placa FROM filtered_plates)
),

-- Calcula diferença de tempo entre registros consecutivos
base_time_diff_data AS (
  SELECT
    *,
    LEAD(datahora) OVER (PARTITION BY placa ORDER BY datahora) AS next_datahora,
    LEAD(camera_numero) OVER (PARTITION BY placa ORDER BY datahora) AS next_camera_numero,
    LEAD(camera_latitude) OVER (PARTITION BY placa ORDER BY datahora) AS next_latitude,
    LEAD(camera_longitude) OVER (PARTITION BY placa ORDER BY datahora) AS next_longitude,
    TIMESTAMP_DIFF(LEAD(datahora) OVER (PARTITION BY placa ORDER BY datahora), datahora, SECOND) AS time_diff
  FROM
    valid_data_with_multiple_plates
),

-- Calcula a distância geodésica entre pontos consecutivos
geo_distance_calculation AS (
  SELECT
    *,
    ST_DISTANCE(
      ST_GEOGPOINT(camera_longitude, camera_latitude),
      ST_GEOGPOINT(next_longitude, next_latitude)
    ) / 1000 AS distance_km
  FROM
    base_time_diff_data
),

-- Calcula velocidade baseada na distância e tempo
velocity_calculation AS (
  SELECT
    *,
    CASE
      WHEN time_diff > 0 THEN distance_km / (time_diff / 3600)
      ELSE NULL
    END AS velocity_kmh
  FROM
    geo_distance_calculation
),

-- Identifica placas com múltiplos tipos de veículos
multiple_vehicle_types AS (
  SELECT
    placa,
    COUNT(DISTINCT tipoveiculo) AS vehicle_type_count
  FROM
    valid_data_with_multiple_plates
  GROUP BY
    placa
  HAVING
    vehicle_type_count > 1
),

-- Identifica possíveis placas clonadas
possible_clones AS (
  SELECT
    v.*,
    mv.placa IS NOT NULL AS multiple_vehicle_types
  FROM
    velocity_calculation v
  LEFT JOIN
    multiple_vehicle_types mv
  ON
    v.placa = mv.placa
  WHERE
    (v.velocity_kmh > 70  AND v.time_diff< 3600)
    OR mv.placa IS NOT NULL
)

SELECT
  *
FROM
  possible_clones
ORDER BY
  placa, datahora;
