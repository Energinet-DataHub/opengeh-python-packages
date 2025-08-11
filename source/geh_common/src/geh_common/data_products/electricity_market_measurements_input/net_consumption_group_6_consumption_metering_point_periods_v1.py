import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_measurements_input"

view_name = "net_consumption_group_6_consumption_metering_point_periods_v1"

schema = T.StructType(
    [
        #
        # GSRN number
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # States whether the metering point has electrical heating in the period
        # true:  The consumption metering has electrical heating in the stated period
        # false: The consumption metering has no electrical heating in the stated period
        T.StructField("has_electrical_heating", T.BooleanType(), not nullable),
        #
        # the scheduled meter reading date for net settlement group 6.
        # The number of the month. 1 is January, 12 is December.
        # 1 | 2 | 3 | ... | 12
        T.StructField("settlement_month", T.IntegerType(), not nullable),
        #
        # See the description of periodization of data above.
        # UTC time
        T.StructField("period_from_date", T.TimestampType(), not nullable),
        #
        # See the description of periodization of data above.
        # UTC time
        T.StructField("period_to_date", T.TimestampType(), nullable),
        #
        # States whether the period was created due to a move-in.
        # Boolean.
        T.StructField("move_in", T.BooleanType(), not nullable),
    ]
)
"""
Consumption (parent) metering points in netsettlement group 6.
The data is periodized; the following transaction types are relevant for determining the periods:
- CHANGESUP: Leverandørskift (BRS-001)
- ENDSUPPLY: Leveranceophør (BRS-002)
- INCCHGSUP: Håndtering af fejlagtigt leverandørskift (BRS-003)
- MSTDATSBM: Fremsendelse af stamdata (BRS-006) - Skift af nettoafregningsgrupper
- MOVEINES: Tilflytning - meldt til elleverandøren (BRS-009)
- MOVEOUTES: Fraflytning - meldt til elleverandøren (BRS-010)
- INCMOVEAUT: Fejlagtig flytning - Automatisk (BRS-011)
- INCMOVEMAN: Fejlagtig flytning - Manuel (BRS-011) HTX
- MDCNSEHON: Oprettelse af elvarme (BRS-015) Det bliver til BRS-041 i DH3
- MDCNSEHOFF: Fjernelse af elvarme (BRS-015) Det bliver til BRS-041 i DH3
- CHGSUPSHRT: Leverandørskift med kort varsel (BRS-043). Findes ikke i DH3
- MANCHGSUP: Tvunget leverandørskifte på målepunkt (BRS-044).
- MANCOR (HTX): Manuelt korrigering
Periods are included when:
- the parent metering point is in netsettlement group 6
- the metering point physical status is connected or disconnected
- an energy supplier is registered
- the period does not end before 2021-01-01

Formatting is according to ADR-144 with the following constraints:
- No column may use quoted values
- All date/time values must include seconds
"""
