import pyspark.sql.types as T

nullable = True

database_name = "electricity_market_measurements_input"

view_name = "electrical_heating_consumption_metering_point_periods_v1"

schema = T.StructType(
    [
        #
        # GSRN number
        T.StructField("metering_point_id", T.StringType(), not nullable),
        #
        # 1 | 2 | 3 | 4 | 5 | 6 | 99 | NULL
        T.StructField("net_settlement_group", T.IntegerType(), nullable),
        #
        # Settlement month is 1st of January for all consumption with electrical heating except for
        # net settlement group 6, where the date is the scheduled meter reading date.
        # The number of the month. 1 is January, 12 is December.
        # For all but settlement group 6 the month is January.
        # 1 | 2 | 3 | ... | 12
        T.StructField(
            "settlement_month",
            T.IntegerType(),
            not nullable,
        ),
        #
        # See the description of periodization of data above.
        # UTC time
        T.StructField("period_from_date", T.TimestampType(), not nullable),
        #
        # See the description of periodization of data above.
        # UTC time
        T.StructField("period_to_date", T.TimestampType(), nullable),
    ]
)
"""
Consumption (parent) metering points related to electrical heating.
The data is periodized; the following transaction types are relevant for determining the periods:
- CHANGESUP: Leverandørskift (BRS-001)
- ENDSUPPLY: Leveranceophør (BRS-002)
- INCCHGSUP: Håndtering af fejlagtigt leverandørskift (BRS-003)
- MSTDATSBM: Fremsendelse af stamdata (BRS-006) - Skift af nettoafregningsgrupper
- LNKCHLDMP: Tilkobling af D15 til parent i nettoafregningsgruppe 2
- ULNKCHLDMP: Afkobling af D15 af parent i nettoafregningsgruppe 2
- ULNKCHLDMP: Afkobling af D14 af parent
- MOVEINES: Tilflytning - meldt til elleverandøren (BRS-009)
- MOVEOUTES: Fraflytning - meldt til elleverandøren (BRS-010)
- INCMOVEAUT: Fejlagtig flytning - Automatisk (BRS-011)
- INCMOVEMAN: Fejlagtig flytning - Manuel (BRS-011) HTX
- MDCNSEHON: Oprettelse af elvarme (BRS-015) Det bliver til BRS-041 i DH3
- MDCNSEHOFF: Fjernelse af elvarme (BRS-015) Det bliver til BRS-041 i DH3
- CHGSUPSHRT: Leverandørskift med kort varsel (BRS-043). Findes ikke i DH3
- MANCHGSUP: Tvunget leverandørskifte på målepunkt (BRS-044).
- MANCOR (HTX): Manuelt korrigering
Periods are  included when
- the metering point physical status is connected or disconnected
- the period does not end before 2021-01-01
- the electrical heating is or has been registered for the period

Formatting is according to ADR-144 with the following constraints:
- No column may use quoted values
- All date/time values must include seconds
"""
