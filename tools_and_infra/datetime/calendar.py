import datetime
from typing import List

import pandas as pd
import math
from yahoo_fin import options
import exchange_calendars as tc

STANDARD_MM_DD_YYYY = '%B %d, %Y'
DEFAULT_TRADING_CALENDAR = 'XYNS'
DEFAULT_EXCHANGE_CALENDAR = tc.get_calendar(DEFAULT_TRADING_CALENDAR)


class CalendarException(Exception):
    pass


class CalendarDate:
    def __init__(self, date: pd.Timestamp, exchange: str = DEFAULT_TRADING_CALENDAR):
        if exchange not in tc.get_calendar_names():
            raise CalendarException('Exchange [' + exchange + '] provided is not registered, please use ISO-10383 ' +
                                    'identifier.')
        if exchange == DEFAULT_TRADING_CALENDAR:
            self.calendar: tc.exchange_calendar.ExchangeCalendar = DEFAULT_EXCHANGE_CALENDAR
        else:
            self.calendar: tc.exchange_calendar.ExchangeCalendar = tc.get_calendar(exchange)
        self._exchange = exchange
        self.date: pd.Timestamp = self.calendar.session_open(date)

    def get_week_of_month(self) -> int:
        first_day = self.date.replace(day=1)
        dom = self.date.day
        adjusted_dom = (1 + first_day.weekday()) % 7
        return int(math.ceil(adjusted_dom / 7.0))

    @classmethod
    def get_available_calendars(cls) -> List[str]:
        return tc.get_calendar_names()

    def get_trading_days_between(self, end: "CalendarDate"):
        return self.calendar.sessions_in_range(self.date, end.date)

    def get_number_of_trading_days_between(self, end: "CalendarDate"):
        return len(self.get_trading_days_between(end))

    def get_day_of_week(self) -> int:
        """
        :param date: the date
        :return: the iso day of the week, starting at 1 (Monday) and ending at 7 (Sunday)
        """
        return self.date.isoweekday()

    def is_trading_day(self) -> bool:
        return self.calendar.is_session(self.date)

    def get_next_trading_day(self) -> "CalendarDate":
        return self.shift_trading_day_by_delta(1)

    def get_previous_trading_day(self) -> "CalendarDate":
        return self.shift_trading_day_by_delta(-1)

    def shift_trading_day_by_delta(self, delta: int) -> "CalendarDate":
        next_day = self.date
        increment_date = self.calendar.next_open if delta > 0 else self.calendar.previous_open
        abs_delta = abs(delta)
        while abs_delta > 0:
            next_day = increment_date(next_day)
            abs_delta = abs_delta - 1
        return CalendarDate(next_day, self._exchange)

    def is_forward_options_expiration(self, ticker: str) -> bool:
        """
        Returns the options expiration date (currently from Yahoo Finance) if it exists.
        Note: this will only work on US Equities and only for future datetimes.
        """
        dates = options.get_expiration_dates(ticker)
        if not dates or dates == ['\n']:
            raise CalendarException('Options expiration data not found for ticker: [' + ticker + '].')
        parsed_date = self.date.strftime(STANDARD_MM_DD_YYYY)
        return parsed_date in dates

    def is_forward_futures_expiration(self, ticker: str) -> bool:
        pass