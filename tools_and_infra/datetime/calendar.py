from __future__ import annotations

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
        """Instantiate a 'CalendarDate' at 'date' using 'exchange'

        Parameters
        ----------
        date : pd.Timestamp
            Standard datetime object
        exchange : str, optional
            Name string of trading exchange, by default DEFAULT_TRADING_CALENDAR

        Raises
        ------
        CalendarException
            Invalid trading exchange given
        """

        if exchange not in tc.get_calendar_names():
            raise CalendarException(f"Exchange ['{exchange}'] provided is not registered, please use ISO-10383 " +
                                    "identifier.")
        if exchange == DEFAULT_TRADING_CALENDAR:
            self.calendar: tc.exchange_calendar.ExchangeCalendar = DEFAULT_EXCHANGE_CALENDAR
        else:
            self.calendar: tc.exchange_calendar.ExchangeCalendar = tc.get_calendar(exchange)
        self._exchange = exchange
        self.date: pd.Timestamp = self.calendar.session_open(date)

    def get_week_of_month(self) -> int:
        """Get week of month

        Returns
        -------
        int
            The week number of the month of 'self.date'
        """

        first_day = self.date.replace(day=1)
        dom = self.date.day
        adjusted_dom = (1 + first_day.weekday()) % 7
        return int(math.ceil(adjusted_dom / 7.0))

    @classmethod
    def get_available_calendars(cls) -> List[str]:
        """Get available exchange calendar names

        Returns
        -------
        List[str]
            List of exchange calendar names as strings
            used during init of a 'CalendarDate' object
        """

        return tc.get_calendar_names()

    def get_trading_days_between(self, end: CalendarDate) -> pd.DatetimeIndex:
        """Get the trading days between 'CalendarDates'

        Parameters
        ----------
        end : CalendarDate
            The desired ending 'CalendarDate'

        Returns
        -------
        pd.DatetimeIndex
            The trading days of 'self._exchange' between 'self.date' and 'end.date'
        """

        if self.date > end.date:
            raise CalendarException(f"Start date {self.date} is after end date {end.date}")
        return self.calendar.sessions_in_range(self.date, end.date)

    def get_number_of_trading_days_between(self, end: CalendarDate) -> int:
        """Get the number of trading days between 'CalendarDates'

        Parameters
        ----------
        end : CalendarDate
            The desired ending 'CalendarDate'

        Returns
        -------
        int
            The number of trading days of 'self._exchange' between
            'self.date' and 'end.date'
        """

        return len(self.get_trading_days_between(end))

    def get_day_of_week(self) -> int:
        """Get day of week

        Returns
        -------
        int
            Integer day of week with Monday starting at 1
            and Sunday ending at 7
        """

        return self.date.isoweekday()

    def is_trading_day(self) -> bool:
        """Checks if current 'self.date' is a trading day of 'self._exchange'

        Returns
        -------
        bool
            True if valid trading day, else False
        """

        return self.calendar.is_session(self.date)

    def get_next_trading_day(self) -> CalendarDate:
        """Gets the next valid trading day

        Finds the next valid trading day from 'self.date' using
        dates from 'self._exchange'

        Returns
        -------
        CalendarDate
            A new 'CalendarDate' object with the date as the next
            valid trading day
        """

        return self.shift_trading_day_by_delta(1)

    def get_previous_trading_day(self) -> CalendarDate:
        """Gets the previous valid trading day

        Finds the previous valid trading day from 'self.date' using
        dates from 'self._exchange'

        Returns
        -------
        CalendarDate
            A new 'CalendarDate' object with the date as the previous
            valid trading day
        """

        return self.shift_trading_day_by_delta(-1)

    def shift_trading_day_by_delta(self, delta: int) -> CalendarDate:
        """Shifts 'CalendarDate' day by 'delta'

        Parameters
        ----------
        delta : int
            Days to increase 'CalendarDate' by

        Returns
        -------
        CalendarDate
            A new 'CalendarDate' object with the date shifted by
            'delta' days ahead of the old 'CalendarDate'
        """
        
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