from datetime import datetime

year_month_day_date_time_formatting = "%Y-%m-%dT%H:%M:%S.%f"
day_month_year_date_time_formatting = "%d-%m-%YT%H:%M:%S%z"
date_formatting = "%d-%m-%Y"


def format_datetime(datetime_str: str) -> datetime:
    """
    date string has to be in format 'dd-mm-YYYYTHH:MM:SS+zzzz'

    date string example: '01-01-2021T00:00:00+0000'
    """

    return datetime.strptime(datetime_str, day_month_year_date_time_formatting)


def format_date(date_str: str) -> datetime:
    """
    date string has to be in format 'dd-mm-YYYY'

    date string example: '01-01-2021'
    """
    return datetime.strptime(date_str, date_formatting)


def convert_to_datetime(datetime_str: str) -> datetime:
    return datetime.strptime(datetime_str[:23], year_month_day_date_time_formatting)


def convert_to_string(datetime: datetime) -> str:
    return datetime.strftime(year_month_day_date_time_formatting)
