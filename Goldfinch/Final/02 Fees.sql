WITH
params AS (
  SELECT TO_DATE('2021-08-19') AS start_day, CURRENT_DATE() AS end_day
),

pools AS (
  SELECT addr FROM (
    VALUES
      ('0x8bbd80f88e662e56b918c353da635e210ece93c6'),
      ('0x1e73b5c1a3570b362d46ae9bf429b25c05e514a7'),
      ('0x95715d3dcbb412900deaf91210879219ea84b4f8'),
      ('0x0e2e11dc77bbe75b2b65b57328a8e4909f7da1eb'),
      ('0x7bdf2679a9f3495260e64c0b9e0dfeb859bad7e0'),
      ('0x4b2ae066681602076adbe051431da7a3200166fd'),
      ('0x1cc90f7bb292dab6fa4398f3763681cfe497db97'),
      ('0x3634855ec1beaf6f9be0f7d2f67fc9cb5f4eeea4'),
      ('0x67df471eacd82c3dbc95604618ff2a1f6b14b8a1'),
      ('0x2107ade0e536b8b0b85cca5e0c0c3f66e58c053c'),
      ('0x9e8b9182abba7b4c188c979bc8f4c79f7f4c90d3'),
      ('0xfce88c5d0ec3f0cb37a044738606738493e9b450'),
      ('0xd798d527f770ad920bb50680dbc202bb0a1dafd6'),
      ('0xe32c22e4d95cae1fb805c60c9e0026ed57971bcf'),
      ('0xefeb69edf6b6999b0e3f2fa856a2acf3bdea4ab5'),
      ('0xc13465ce9ae3aa184eb536f04fdc3f54d2def277'),
      ('0xaa2ccc5547f64c5dffd0a624eb4af2543a67ba65'),
      ('0xf74ea34ac88862b7ff419e60e476be2651433e68'),
      ('0xc9bdd0d3b80cc6efe79a82d850f44ec9b55387ae'),
      ('0xe6c30756136e07eb5268c3232efbfbe645c1ba5a'),
      ('0x1d596d28a7923a22aa013b0e7082bba23daa656b'),
      ('0x6b42b1a43abe9598052bb8c21fd34c469fbcb8b'),
      ('0x418749e294cabce5a714efccc22a8aade6f9db57'),
      ('0xa49506632ce8ec826b0190262b89a800353675ec'),
      ('0x00c27fc71b159a346e179b4a1608a0865e8a7470'),
      ('0xd09a57127bc40d680be7cb061c2a6629fe71abef'),
      ('0xb26b42dd5771689d0a7faeea32825ff9710b9c11'),
      ('0x759f097f3153f5d62ff1c2d82ba78b6350f223e3'),
      ('0x89d7c618a4eef3065da8ad684859a547548e6169'),
      ('0xd43a4f3041069c6178b99d55295b00d0db955bb5'),
      ('0x294371f9ec8b6ddf59d4a2ceba377d19b9735d34'),
      ('0x538473c3a69da2b305cf11a40cf2f3904de8db5f')
  ) v(addr)
),

core AS (   
  SELECT '0xb01b315e32d1d9b5ce93e296d483e1f0aad39e75' AS addr
),
senior AS ( 
  SELECT '0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822' AS addr
),

days AS (
  SELECT day
  FROM (
    SELECT DATEADD(day, SEQ4(), (SELECT start_day FROM params)) AS day
    FROM TABLE(GENERATOR(ROWCOUNT => 5000))
  )
  WHERE day <= (SELECT end_day FROM params)
),

interest_collected AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM( COALESCE(TRY_TO_NUMBER(decoded_log:poolAmount::string), 0) / 1e6 ) AS usd_ic
  FROM ethereum.core.ez_decoded_event_logs
  WHERE tx_succeeded = 'TRUE'
    AND event_name = 'InterestCollected'
    AND LOWER(contract_address) IN (SELECT addr FROM core)
    AND block_timestamp >= (SELECT start_day::timestamp_ntz FROM params)
    AND block_timestamp <  ((SELECT end_day::timestamp_ntz FROM params) + INTERVAL '1 DAY')
  GROUP BY 1
),

payment_applied AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM( (COALESCE(TRY_TO_NUMBER(decoded_log:interestAmount::string), 0)
        - COALESCE(TRY_TO_NUMBER(decoded_log:reserveAmount::string), 0)) / 1e6 ) AS usd_pa
  FROM ethereum.core.ez_decoded_event_logs
  WHERE tx_succeeded = 'TRUE'
    AND event_name = 'PaymentApplied'
    AND LOWER(contract_address) IN (SELECT addr FROM pools)
    AND block_timestamp >= (SELECT start_day::timestamp_ntz FROM params)
    AND block_timestamp <  ((SELECT end_day::timestamp_ntz FROM params) + INTERVAL '1 DAY')
  GROUP BY 1
),

reserve_collected AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM( COALESCE(TRY_TO_NUMBER(decoded_log:amount::string), 0) / 1e6 ) AS usd_rc
  FROM ethereum.core.ez_decoded_event_logs
  WHERE tx_succeeded = 'TRUE'
    AND event_name = 'ReserveFundsCollected'
    AND LOWER(contract_address) IN (
      SELECT addr FROM pools
      UNION ALL SELECT addr FROM core
      UNION ALL SELECT addr FROM senior
    )
    AND block_timestamp >= (SELECT start_day::timestamp_ntz FROM params)
    AND block_timestamp <  ((SELECT end_day::timestamp_ntz FROM params) + INTERVAL '1 DAY')
  GROUP BY 1
)

SELECT
  d.day,
  COALESCE(ic.usd_ic, 0) + COALESCE(pa.usd_pa, 0) + COALESCE(rc.usd_rc, 0) AS daily_fees_usd
FROM days d
LEFT JOIN interest_collected ic USING (day)
LEFT JOIN payment_applied  pa USING (day)
LEFT JOIN reserve_collected rc USING (day)
ORDER BY d.day desc;