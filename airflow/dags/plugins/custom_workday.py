#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Plugin to demonstrate timetable registration and accommodate example DAGs."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from airflow.utils.types import DagRunType
# [START howto_timetable]
from pendulum import UTC, Date, DateTime, Time

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable

if TYPE_CHECKING:
    from airflow.timetables.base import TimeRestriction

log = logging.getLogger(__name__)
try:
    from pandas.tseries.holiday import USFederalHolidayCalendar

    holiday_calendar = USFederalHolidayCalendar()
except ImportError:
    log.warning("Could not import pandas. Holidays will not be considered.")
    holiday_calendar = None  # type: ignore[assignment]


class SometimeAfterWorkdayTimetable(Timetable):
    def __init__(self, schedule_at: Time) -> None:
        self._schedule_at = schedule_at
        self.description = "Schedule: after each workday, at f{self._schedule_at}"

    @property
    def summary(self) -> str:
        return f"after each workday, at {self._schedule_at}"

    def serialize(self) -> dict[str, Any]:
        return {"schedule_at": self._schedule_at.isoformat()}

    @classmethod
    def deserialize(cls, value: dict[str, Any]) -> Timetable:
        return cls(Time.fromisoformat(value["schedule_at"]))

    def get_next_workday(self, d: DateTime, incr=1) -> DateTime:
        next_start = d
        while True:
            if next_start.weekday() not in (5, 6):  # not on weekend
                if holiday_calendar is None:
                    holidays = set()
                else:
                    holidays = holiday_calendar.holidays(start=next_start, end=next_start).to_pydatetime()
                if next_start not in holidays:
                    break
            next_start = next_start.add(days=incr)
        return next_start

    # [START howto_timetable_infer_manual_data_interval]
    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        start = DateTime.combine((run_after - timedelta(days=1)).date(), Time.min).replace(tzinfo=UTC)
        # Skip backwards over weekends and holidays to find last run
        start = self.get_next_workday(start, incr=-1)
        return DataInterval(start=start, end=(start + timedelta(days=1)))

    # [END howto_timetable_infer_manual_data_interval]

    # [START howto_timetable_next_dagrun_info]
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            next_start = DateTime.combine((last_start + timedelta(days=1)).date(), Time.min)
        # Otherwise this is the first ever run on the regular schedule...
        elif (earliest := restriction.earliest) is None:
            return None  # No start_date. Don't schedule.
        elif not restriction.catchup:
            # If the DAG has catchup=False, today is the earliest to consider.
            next_start = max(earliest, DateTime.combine(Date.today(), Time.min))
        elif earliest.time() != Time.min:
            # If earliest does not fall on midnight, skip to the next day.
            next_start = DateTime.combine(earliest.date() + timedelta(days=1), Time.min)
        else:
            next_start = earliest
        # Skip weekends and holidays
        next_start = self.get_next_workday(next_start.replace(tzinfo=UTC))

        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        #return DagRunInfo.interval(start=next_start, end=(next_start + timedelta(days=1)))
        end = next_start + timedelta(days=1)
        return DagRunInfo(
            data_interval=DataInterval(start=next_start, end=end),
            run_after=DateTime.combine(end.date(), self._schedule_at).replace(tzinfo=UTC)
        )

    # [END howto_timetable_next_dagrun_info]

    def generate_run_id(
            self,
            *,
            run_type: DagRunType,
            logical_date: DateTime,
            data_interval: DataInterval | None,
            **extra,
    ) -> str:
        if run_type == DagRunType.SCHEDULED and data_interval:
            return data_interval.end.format("YYYY-MM-DD dddd")
        return super().generate_run_id(
            run_type=run_type, logical_date=logical_date, data_interval=data_interval, **extra
        )


class CustomWorkdayTimetablePlugin(AirflowPlugin):
    name = "custom_workday_timetable_plugin"
    timetables = [SometimeAfterWorkdayTimetable]


# [END howto_timetable]
