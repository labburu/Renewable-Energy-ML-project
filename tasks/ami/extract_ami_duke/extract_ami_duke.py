

class ExtractAmiDuke():

    def run(self, spark):
        """Read Duke AMI csvs and apply column naming and types."""
        df = spark.sql("""
            SELECT
                lpad(cast(trim(_c0) as varchar(20)), 20, '0') AS external_residence_id,
                lpad(cast(trim(_c1) as varchar(20)), 20, '0') AS internal_duke_id,
                lpad(cast(trim(_c2) as varchar(20)), 20, '0') AS external_channel_id,
                cast(_c3 as timestamp)                        AS meter_read_time_edt,
                cast(_c4 as timestamp)                        AS meter_read_time_eastern,
                cast(_c5 as decimal(16, 3))                   AS consumption,
                cast(_c6 as int)                              AS seconds_per_interval,
                cast(trim(_c7) as varchar(10))                AS unit_of_measure,
                cast(trim(_c8) as varchar(10))                AS jurisdiction,
                cast(trim(_c9) as varchar(1))                 AS direction,
                cast(trim(_c10) as varchar(3))                AS code
            FROM
                ami
        """)

        df.createOrReplaceTempView('output')
