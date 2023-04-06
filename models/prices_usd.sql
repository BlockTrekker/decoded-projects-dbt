


-- Generate a list of minutes between the first and last day of trading for each token
minutes as (
  SELECT
    GENERATE_TIMESTAMP_ARRAY(
      DATE_TRUNC(MINUTE, MIN(CAST(day as DATE))),
      DATE_TRUNC(MINUTE, MAX(CAST(day as DATE))),
      INTERVAL 1 MINUTE
    ) AS minute
  FROM
    {{ref('price_usd_daily')}} t
  GROUP BY
    symbol
)

-- For each minute, calculate the price by interpolating between the nearest prices available in the price per day table

SELECT
    m.symbol,
    m.minute,
    t.price
FROM
    {{ref('price_usd_daily')}} t
LEFT JOIN minutes m
    ON
        minutes.symbol = prices.symbol AND
        DATE_TRUNC(minutes.minutes) = CAST(day as DATE)
ORDER BY
  symbol,
  minute

