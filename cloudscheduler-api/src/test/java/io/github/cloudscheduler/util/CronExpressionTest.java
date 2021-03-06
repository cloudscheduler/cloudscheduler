/*
 * Copyright (c) 2018. cloudscheduler
 * All rights reserved.
 *
 * Permission is hereby granted, free  of charge, to any person obtaining
 * a  copy  of this  software  and  associated  documentation files  (the
 * "Software"), to  deal in  the Software without  restriction, including
 * without limitation  the rights to  use, copy, modify,  merge, publish,
 * distribute,  sublicense, and/or sell  copies of  the Software,  and to
 * permit persons to whom the Software  is furnished to do so, subject to
 * the following conditions:
 *
 * The  above  copyright  notice  and  this permission  notice  shall  be
 * included in all copies or substantial portions of the Software.
 *
 * THE  SOFTWARE IS  PROVIDED  "AS  IS", WITHOUT  WARRANTY  OF ANY  KIND,
 * EXPRESS OR  IMPLIED, INCLUDING  BUT NOT LIMITED  TO THE  WARRANTIES OF
 * MERCHANTABILITY,    FITNESS    FOR    A   PARTICULAR    PURPOSE    AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE,  ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.github.cloudscheduler.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Wei Gao */
public class CronExpressionTest {
  private TimeZone original;
  private ZoneId zoneId;

  @BeforeEach
  public void setUp() {
    original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    zoneId = TimeZone.getDefault().toZoneId();
  }

  @AfterEach
  public void tearDown() {
    TimeZone.setDefault(original);
  }

  @Test
  public void shall_parse_number() {
    CronExpression.SimpleField field =
        new CronExpression.SimpleField(CronExpression.CronFieldType.MINUTE, "5");
    assertPossibleValues(field, 5);
  }

  private void assertPossibleValues(CronExpression.SimpleField field, Integer... values) {
    Set<Integer> valid = values == null ? new HashSet<>() : new HashSet<>(Arrays.asList(values));
    for (int i = field.fieldType.from; i <= field.fieldType.to; i++) {
      String errorText = i + ":" + valid;
      if (valid.contains(i)) {
        assertThat(field.matches(i)).as(errorText).isTrue();
      } else {
        assertThat(field.matches(i)).as(errorText).isFalse();
      }
    }
  }

  @Test
  public void shall_parse_number_with_increment() {
    CronExpression.SimpleField field =
        new CronExpression.SimpleField(CronExpression.CronFieldType.MINUTE, "0/15");
    assertPossibleValues(field, 0, 15, 30, 45);
  }

  @Test
  public void shall_parse_range() {
    CronExpression.SimpleField field =
        new CronExpression.SimpleField(CronExpression.CronFieldType.MINUTE, "5-10");
    assertPossibleValues(field, 5, 6, 7, 8, 9, 10);
  }

  @Test
  public void shall_parse_range_with_increment() {
    CronExpression.SimpleField field =
        new CronExpression.SimpleField(CronExpression.CronFieldType.MINUTE, "20-30/2");
    assertPossibleValues(field, 20, 22, 24, 26, 28, 30);
  }

  @Test
  public void shall_parse_asterix() {
    CronExpression.SimpleField field =
        new CronExpression.SimpleField(CronExpression.CronFieldType.DAY_OF_WEEK, "*");
    assertPossibleValues(field, 1, 2, 3, 4, 5, 6, 7);
  }

  @Test
  public void shall_parse_asterix_with_increment() {
    CronExpression.SimpleField field =
        new CronExpression.SimpleField(CronExpression.CronFieldType.DAY_OF_WEEK, "*/2");
    assertPossibleValues(field, 1, 3, 5, 7);
  }

  @Test
  public void shall_ignore_field_in_day_of_week() {
    CronExpression.DayOfWeekField field = new CronExpression.DayOfWeekField("?");
    assertThat(field.matches(ZonedDateTime.now().toLocalDate())).as("day of week is ?").isTrue();
  }

  @Test
  public void shall_ignore_field_in_day_of_month() {
    CronExpression.DayOfMonthField field = new CronExpression.DayOfMonthField("?");
    assertThat(field.matches(ZonedDateTime.now().toLocalDate())).as("day of month is ?").isTrue();
  }

  @Test
  public void shall_give_error_if_invalid_count_field() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("* 3 *"));
  }

  @Test
  public void shall_give_error_if_minute_field_ignored() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              CronExpression.SimpleField field =
                  new CronExpression.SimpleField(CronExpression.CronFieldType.MINUTE, "?");
              field.matches(1);
            });
  }

  @Test
  public void shall_give_error_if_hour_field_ignored() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              CronExpression.SimpleField field =
                  new CronExpression.SimpleField(CronExpression.CronFieldType.HOUR, "?");
              field.matches(1);
            });
  }

  @Test
  public void shall_give_error_if_month_field_ignored() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              CronExpression.SimpleField field =
                  new CronExpression.SimpleField(CronExpression.CronFieldType.MONTH, "?");
              field.matches(1);
            });
  }

  @Test
  public void shall_give_last_day_of_month_in_leapyear() {
    CronExpression.DayOfMonthField field = new CronExpression.DayOfMonthField("L");
    assertThat(field.matches(LocalDate.of(2012, 2, 29))).as("day of month is L").isTrue();
  }

  @Test
  public void shall_give_last_day_of_month() {
    CronExpression.DayOfMonthField field = new CronExpression.DayOfMonthField("L");
    YearMonth now = YearMonth.now();
    assertThat(field.matches(LocalDate.of(now.getYear(), now.getMonthValue(), now.lengthOfMonth())))
        .as("L matches to the last day of month")
        .isTrue();
  }

  @Test
  public void check_all() {
    CronExpression cronExpr = new CronExpression("* * * * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 1, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 2, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 2, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 2, 1, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 59, 59, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 14, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_invalid_input() {
    assertThatNullPointerException().isThrownBy(() -> new CronExpression(null));
  }

  @Test
  public void check_second_number() {
    CronExpression cronExpr = new CronExpression("3 * * * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 1, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 13, 1, 3, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 1, 3, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 2, 3, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 59, 3, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 14, 0, 3, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 23, 59, 3, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 11, 0, 0, 3, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 30, 23, 59, 3, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 1, 0, 0, 3, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_second_increment() {
    CronExpression cronExpr = new CronExpression("5/15 * * * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 5, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 5, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 20, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 20, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 35, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 35, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 50, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 50, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 1, 5, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    // if rolling over minute then reset second (cron rules - increment affects only values in own
    // field)
    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 50, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 1, 10, 0, zoneId);
    assertThat(new CronExpression("10/100 * * * * *").nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 1, 10, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 2, 10, 0, zoneId);
    assertThat(new CronExpression("10/100 * * * * *").nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_second_list() {
    CronExpression cronExpr = new CronExpression("7,19 * * * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 7, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 7, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 19, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 19, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 1, 7, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_second_range() {
    CronExpression cronExpr = new CronExpression("42-45 * * * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 42, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 42, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 43, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 43, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 44, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 44, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 0, 45, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 0, 45, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 1, 42, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_second_invalid_range() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("42-63 * * * * *"));
  }

  @Test
  public void check_second_invalid_increment_modifier() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("42#3 * * * * *"));
  }

  @Test
  public void check_minute_number() {
    CronExpression cronExpr = new CronExpression("0 3 * * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 1, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 13, 3, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 3, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 14, 3, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_minute_increment() {
    CronExpression cronExpr = new CronExpression("0 0/15 * * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 13, 15, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 15, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 30, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 30, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 45, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 45, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 14, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_minute_list() {
    CronExpression cronExpr = new CronExpression("0 7,19 * * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 13, 7, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 13, 7, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 13, 19, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_hour_number() {
    CronExpression cronExpr = new CronExpression("0 * 3 * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 1, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 11, 3, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 11, 3, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 11, 3, 1, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 11, 3, 59, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 12, 3, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_hour_increment() {
    CronExpression cronExpr = new CronExpression("0 * 0/15 * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 15, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 15, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 15, 1, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 15, 59, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 11, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 11, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 11, 0, 1, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 11, 15, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 11, 15, 1, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_hour_list() {
    CronExpression cronExpr = new CronExpression("0 * 7,19 * * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 10, 19, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 19, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 10, 19, 1, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 10, 19, 59, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 11, 7, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_hour_shall_run_25_times_in_DST_change_to_wintertime() {
    CronExpression cron = new CronExpression("0 1 * * * *");
    ZonedDateTime start = ZonedDateTime.of(2011, 11, 6, 0, 0, 0, 0, zoneId);
    ZonedDateTime end = start.plusDays(1);

    int count = getCount(cron, start, end);
    assertThat(count).isEqualTo(25);
  }

  @Test
  public void check_hour_shall_run_23_times_in_DST_change_to_summertime() {
    CronExpression cron = new CronExpression("0 0 * * * *");
    ZonedDateTime start = ZonedDateTime.of(2011, 3, 13, 1, 0, 0, 0, zoneId);
    ZonedDateTime end = start.plusDays(1);

    int count = getCount(cron, start, end);
    assertThat(count).isEqualTo(23);
  }

  private int getCount(CronExpression cron, ZonedDateTime startTime, ZonedDateTime endTime) {
    int count = 0;
    ZonedDateTime tid = startTime;
    ZonedDateTime lastTime = startTime;
    while (tid.isBefore(endTime)) {
      ZonedDateTime nextTime = cron.nextTimeAfter(tid);
      assertThat(nextTime.isAfter(lastTime)).isTrue();
      lastTime = nextTime;
      tid = tid.plusHours(1L);
      count++;
    }
    return count;
  }

  @Test
  public void check_dayOfMonth_number() {
    CronExpression cronExpr = new CronExpression("0 * * 3 * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 5, 3, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 5, 3, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 3, 0, 1, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 5, 3, 0, 59, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 3, 1, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 5, 3, 23, 59, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 6, 3, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfMonth_increment() {
    CronExpression cronExpr = new CronExpression("0 0 0 1/15 * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 16, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 16, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 1, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 30, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 5, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 16, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfMonth_list() {
    CronExpression cronExpr = new CronExpression("0 0 0 7,19 * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 19, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 19, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 7, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 5, 7, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 19, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 5, 30, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 6, 7, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfMonth_last() {
    CronExpression cronExpr = new CronExpression("0 0 0 L * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 30, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 2, 12, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 2, 29, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfMonth_number_last_L() {
    CronExpression cronExpr = new CronExpression("0 0 0 3L * *");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 10, 13, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 30 - 3, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 2, 12, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 2, 29 - 3, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfMonth_closest_weekday_W() {
    CronExpression cronExpr = new CronExpression("0 0 0 9W * *");

    // 9 - is weekday in may
    ZonedDateTime after = ZonedDateTime.of(2012, 5, 2, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 5, 9, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    // 9 - is weekday in may
    after = ZonedDateTime.of(2012, 5, 8, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    // 9 - saturday, friday closest weekday in june
    after = ZonedDateTime.of(2012, 5, 9, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 6, 8, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    // 9 - sunday, monday closest weekday in september
    after = ZonedDateTime.of(2012, 9, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 9, 10, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfMonth_invalid_modifier() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("0 0 0 9X * *"));
  }

  @Test
  public void check_dayOfMonth_invalid_increment_modifier() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("0 0 0 9#2 * *"));
  }

  @Test
  public void check_month_number() {
    ZonedDateTime after = ZonedDateTime.of(2012, 2, 12, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 5, 1, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 1 5 *").nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_month_increment() {
    ZonedDateTime after = ZonedDateTime.of(2012, 2, 12, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 5, 1, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 1 5/2 *").nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 5, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 7, 1, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 1 5/2 *").nextTimeAfter(after)).isEqualTo(expected);

    // if rolling over year then reset month field (cron rules - increments only affect own field)
    after = ZonedDateTime.of(2012, 5, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2013, 5, 1, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 1 5/10 *").nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_month_list() {
    CronExpression cronExpr = new CronExpression("0 0 0 1 3,7,12 *");

    ZonedDateTime after = ZonedDateTime.of(2012, 2, 12, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 7, 1, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 7, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 12, 1, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_month_list_by_name() {
    CronExpression cronExpr = new CronExpression("0 0 0 1 MAR,JUL,DEC *");

    ZonedDateTime after = ZonedDateTime.of(2012, 2, 12, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 7, 1, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 7, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 12, 1, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_month_invalid_modifier() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("0 0 0 1 ? *"));
  }

  @Test
  public void check_dayOfWeek_number() {
    CronExpression cronExpr = new CronExpression("0 0 0 * * 3");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 4, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 4, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 11, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 12, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 18, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 18, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 25, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfWeek_increment() {
    CronExpression cronExpr = new CronExpression("0 0 0 * * 3/2");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 4, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 4, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 6, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 6, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 8, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 8, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 11, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfWeek_list() {
    CronExpression cronExpr = new CronExpression("0 0 0 * * 1,5,7");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 2, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 2, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 6, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 6, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 8, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfWeek_list_by_name() {
    CronExpression cronExpr = new CronExpression("0 0 0 * * MON,FRI,SUN");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 2, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 2, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 6, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 6, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 8, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfWeek_last_friday_in_month() {
    CronExpression cronExpr = new CronExpression("0 0 0 * * 5L");

    ZonedDateTime after = ZonedDateTime.of(2012, 4, 1, 1, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 27, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 27, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 25, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 2, 6, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 2, 24, 0, 0, 0, 0, zoneId);
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 2, 6, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 2, 24, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * FRIL").nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfWeek_invalid_modifier() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("0 0 0 * * 5W"));
  }

  @Test
  public void check_dayOfWeek_invalid_increment_modifier() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("0 0 0 * * 5?3"));
  }

  @Test
  public void check_dayOfWeek_shall_interpret_0_as_sunday() {
    ZonedDateTime after = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 8, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 0").nextTimeAfter(after)).isEqualTo(expected);

    expected = ZonedDateTime.of(2012, 4, 29, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 0L").nextTimeAfter(after)).isEqualTo(expected);

    expected = ZonedDateTime.of(2012, 4, 8, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 0#2").nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfWeek_shall_interpret_7_as_sunday() {
    ZonedDateTime after = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 8, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 7").nextTimeAfter(after)).isEqualTo(expected);

    expected = ZonedDateTime.of(2012, 4, 29, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 7L").nextTimeAfter(after)).isEqualTo(expected);

    expected = ZonedDateTime.of(2012, 4, 8, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 7#2").nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void check_dayOfWeek_nth_day_in_month() {
    ZonedDateTime after = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2012, 4, 20, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 5#3").nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 20, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 18, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 5#3").nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 3, 30, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 7#1").nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 4, 1, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 5, 6, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 7#1").nextTimeAfter(after)).isEqualTo(expected);

    after = ZonedDateTime.of(2012, 2, 6, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 2, 29, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * 3#5").nextTimeAfter(after))
        .isEqualTo(expected); // leapday

    after = ZonedDateTime.of(2012, 2, 6, 0, 0, 0, 0, zoneId);
    expected = ZonedDateTime.of(2012, 2, 29, 0, 0, 0, 0, zoneId);
    assertThat(new CronExpression("0 0 0 * * WED#5").nextTimeAfter(after))
        .isEqualTo(expected); // leapday
  }

  @Test
  public void shall_not_not_support_rolling_period() {
    assertThatIllegalArgumentException().isThrownBy(() -> new CronExpression("* * 5-1 * * *"));
  }

  @Test
  public void non_existing_date_throws_exception() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              // Will check for the next 4 years - no 30th of February is found so a IAE is thrown.
              new CronExpression("* * * 30 2 *").nextTimeAfter(ZonedDateTime.now());
            });
  }

  @Test
  public void test_default_barrier() {
    CronExpression cronExpr = new CronExpression("* * * 29 2 *");

    ZonedDateTime after = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2016, 2, 29, 0, 0, 0, 0, zoneId);
    // the default barrier is 4 years - so leap years are considered.
    assertThat(cronExpr.nextTimeAfter(after)).isEqualTo(expected);
  }

  @Test
  public void test_one_year_barrier() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              ZonedDateTime after = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, zoneId);
              ZonedDateTime barrier = ZonedDateTime.of(2013, 3, 1, 0, 0, 0, 0, zoneId);
              // The next leap year is 2016, so an IllegalArgumentException is expected.
              new CronExpression("* * * 29 2 *").nextTimeAfter(after, barrier);
            });
  }

  @Test
  public void test_two_year_barrier() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              ZonedDateTime after = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, zoneId);
              // The next leap year is 2016, so an IllegalArgumentException is expected.
              new CronExpression("* * * 29 2 *")
                  .nextTimeAfter(after, 1000L * 60 * 60 * 24 * 356 * 2);
            });
  }

  @Test
  public void test_seconds_specified_but_should_be_omitted() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CronExpression.createWithoutSeconds("* * * 29 2 *"));
  }

  @Test
  public void test_without_seconds() {
    ZonedDateTime after = ZonedDateTime.of(2012, 3, 1, 0, 0, 0, 0, zoneId);
    ZonedDateTime expected = ZonedDateTime.of(2016, 2, 29, 0, 0, 0, 0, zoneId);
    assertThat(CronExpression.createWithoutSeconds("* * 29 2 *").nextTimeAfter(after))
        .isEqualTo(expected);
  }
}
