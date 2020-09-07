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

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cron expression implementation.
 *
 * <p>Create cron express by static method: {@link #create(String)} or {@link
 * #createWithoutSeconds(String)}. Get next trigger time by method {@link
 * #nextTimeAfter(ZonedDateTime)}, {@link #nextTimeAfter(ZonedDateTime, long)} or {@link
 * #nextTimeAfter(ZonedDateTime, ZonedDateTime)}
 *
 * @author Wei Gao
 */
public class CronExpression {
  private final String expr;
  private final SimpleField secondField;
  private final SimpleField minuteField;
  private final SimpleField hourField;
  private final DayOfWeekField dayOfWeekField;
  private final SimpleField monthField;
  private final DayOfMonthField dayOfMonthField;

  CronExpression(final String expr) {
    this(expr, true);
  }

  CronExpression(final String expr, final boolean withSeconds) {
    Objects.requireNonNull(expr, "expr mandatory"); // $NON-NLS-1$

    this.expr = expr;

    final int expectedParts = withSeconds ? 6 : 5;
    final String[] parts = expr.split("\\s+"); // $NON-NLS-1$
    if (parts.length != expectedParts) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid cron expression [%s], expected %s field, got %s",
              expr, expectedParts, parts.length));
    }

    int ix = withSeconds ? 1 : 0;
    this.secondField = new SimpleField(CronFieldType.SECOND, withSeconds ? parts[0] : "0");
    this.minuteField = new SimpleField(CronFieldType.MINUTE, parts[ix++]);
    this.hourField = new SimpleField(CronFieldType.HOUR, parts[ix++]);
    this.dayOfMonthField = new DayOfMonthField(parts[ix++]);
    this.monthField = new SimpleField(CronFieldType.MONTH, parts[ix++]);
    this.dayOfWeekField = new DayOfWeekField(parts[ix]);
  }

  public static CronExpression create(final String expr) {
    return new CronExpression(expr, true);
  }

  public static CronExpression createWithoutSeconds(final String expr) {
    return new CronExpression(expr, false);
  }

  private static void checkIfDateTimeBarrierIsReached(
      ZonedDateTime nextTime, ZonedDateTime dateTimeBarrier) {
    if (nextTime.isAfter(dateTimeBarrier)) {
      throw new IllegalArgumentException(
          "No next execution time could be determined that is before the limit of "
              + dateTimeBarrier);
    }
  }

  /**
   * Get next time after certain time.
   *
   * @param afterTime after time
   * @return next time
   */
  public ZonedDateTime nextTimeAfter(ZonedDateTime afterTime) {
    // will search for the next time within the next 4 years. If there is no
    // time matching, an InvalidArgumentException will be thrown (it is very
    // likely that the cron expression is invalid, like the February 30th).
    return nextTimeAfter(afterTime, afterTime.plusYears(4));
  }

  /**
   * Get next time after certain time.
   *
   * @param afterTime after time
   * @param durationInMillis duration in million second
   * @return next time
   */
  public ZonedDateTime nextTimeAfter(ZonedDateTime afterTime, long durationInMillis) {
    // will search for the next time within the next durationInMillis
    // millisecond. Be aware that the duration is specified in millis,
    // but in fact the limit is checked on a day-to-day basis.
    return nextTimeAfter(afterTime, afterTime.plus(Duration.ofMillis(durationInMillis)));
  }

  /**
   * Get next time after certain time.
   *
   * @param afterTime after time
   * @param dateTimeBarrier barrier
   * @return next time
   */
  public ZonedDateTime nextTimeAfter(ZonedDateTime afterTime, ZonedDateTime dateTimeBarrier) {
    ZonedDateTime nextTime = ZonedDateTime.from(afterTime).withNano(0).plusSeconds(1).withNano(0);

    while (true) { // day of week
      while (true) { // month
        while (true) { // day of month
          while (true) { // hour
            while (true) { // minute
              while (!secondField.matches(nextTime.getSecond())) { // second
                nextTime = nextTime.plusSeconds(1).withNano(0);
              }
              if (minuteField.matches(nextTime.getMinute())) {
                break;
              }
              nextTime = nextTime.plusMinutes(1).withSecond(0).withNano(0);
            }
            if (hourField.matches(nextTime.getHour())) {
              break;
            }
            nextTime = nextTime.plusHours(1).withMinute(0).withSecond(0).withNano(0);
          }
          if (dayOfMonthField.matches(nextTime.toLocalDate())) {
            break;
          }
          nextTime = nextTime.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
          checkIfDateTimeBarrierIsReached(nextTime, dateTimeBarrier);
        }
        if (monthField.matches(nextTime.getMonth().getValue())) {
          break;
        }
        nextTime =
            nextTime
                .plusMonths(1)
                .withDayOfMonth(1)
                .withHour(0)
                .withMinute(0)
                .withSecond(0)
                .withNano(0);
        checkIfDateTimeBarrierIsReached(nextTime, dateTimeBarrier);
      }
      if (dayOfWeekField.matches(nextTime.toLocalDate())) {
        break;
      }
      nextTime = nextTime.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
      checkIfDateTimeBarrierIsReached(nextTime, dateTimeBarrier);
    }

    return nextTime;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "<" + expr + ">";
  }

  enum CronFieldType {
    SECOND(0, 59, null),
    MINUTE(0, 59, null),
    HOUR(0, 23, null),
    DAY_OF_MONTH(1, 31, null),
    MONTH(
        1,
        12,
        Arrays.asList(
            "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")),
    DAY_OF_WEEK(1, 7, Arrays.asList("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"));

    final int from;
    final int to;
    final List<String> names;

    CronFieldType(int from, int to, List<String> names) {
      this.from = from;
      this.to = to;
      this.names = names;
    }
  }

  static class FieldPart {
    private Integer from;
    private Integer to;
    private Integer increment;
    private String modifier;
    private String incrementModifier;
  }

  abstract static class BasicField {
    private static final Pattern CRON_FIELD_REGEXP =
        Pattern.compile(
            "(?: # start of group 1\n"
                + "(?:(?<all>\\*)|(?<ignore>\\?)|(?<last>L)) # global flag (L, ?, *)\n"
                + " | (?<start>[0-9]{1,2}|[a-z]{3}) # or start number or symbol\n"
                + " (?: # start of group 2\n"
                + " (?<mod>L|W) # modifier (L,W)\n"
                + " | -(?<end>[0-9]{1,2}|[a-z]{3}) # or end nummer or symbol (in range)\n"
                + " )? # end of group 2\n"
                + ") # end of group 1\n"
                + "(?:(?<incmod>/|\\#)(?<inc>[0-9]{1,7}))? # increment and increment modifier"
                + "(/ or \\#)\n",
            Pattern.CASE_INSENSITIVE | Pattern.COMMENTS);

    final CronFieldType fieldType;
    final List<FieldPart> parts = new ArrayList<>();

    private BasicField(CronFieldType fieldType, String fieldExpr) {
      this.fieldType = fieldType;
      parse(fieldExpr);
    }

    private void parse(String fieldExpr) {
      String[] rangeParts = fieldExpr.split(",");
      for (String rangePart : rangeParts) {
        Matcher m = CRON_FIELD_REGEXP.matcher(rangePart);
        if (!m.matches()) {
          throw new IllegalArgumentException(
              "Invalid cron field '" + rangePart + "' for field [" + fieldType + "]");
        }
        String startNumber = m.group("start");
        String modifier = m.group("mod");
        String stopNumber = m.group("end");
        String incrementModifier = m.group("incmod");
        String increment = m.group("inc");

        FieldPart part = new FieldPart();
        part.increment = 999;
        if (startNumber != null) {
          part.from = mapValue(startNumber);
          part.modifier = modifier;
          if (stopNumber != null) {
            part.to = mapValue(stopNumber);
            part.increment = 1;
          } else if (increment != null) {
            part.to = fieldType.to;
          } else {
            part.to = part.from;
          }
        } else if (m.group("all") != null) {
          part.from = fieldType.from;
          part.to = fieldType.to;
          part.increment = 1;
        } else if (m.group("ignore") != null) {
          part.modifier = m.group("ignore");
        } else if (m.group("last") != null) {
          part.modifier = m.group("last");
        } else {
          throw new IllegalArgumentException("Invalid cron part: " + rangePart);
        }

        if (increment != null) {
          part.incrementModifier = incrementModifier;
          part.increment = Integer.valueOf(increment);
        }

        validateRange(part);
        validatePart(part);
        parts.add(part);
      }
    }

    protected void validatePart(FieldPart part) {
      if (part.modifier != null) {
        throw new IllegalArgumentException(String.format("Invalid modifier [%s]", part.modifier));
      } else if (part.incrementModifier != null && !"/".equals(part.incrementModifier)) {
        throw new IllegalArgumentException(
            String.format("Invalid increment modifier [%s]", part.incrementModifier));
      }
    }

    private void validateRange(FieldPart part) {
      if ((part.from != null && part.from < fieldType.from)
          || (part.to != null && part.to > fieldType.to)) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid interval [%s-%s], must be %s<=_<=%s",
                part.from, part.to, fieldType.from, fieldType.to));
      } else if (part.from != null && part.to != null && part.from > part.to) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid interval [%s-%s]. Rolling periods are not supported"
                    + " (ex. 5-1, only 1-5) since this won't give a deterministic result. "
                    + "Must be %s<=_<=%s",
                part.from, part.to, fieldType.from, fieldType.to));
      }
    }

    protected Integer mapValue(String value) {
      int idx;
      if (fieldType.names != null
          && (idx = fieldType.names.indexOf(value.toUpperCase(Locale.getDefault()))) >= 0) {
        return idx + 1;
      }
      return Integer.valueOf(value);
    }

    protected boolean matches(int val, FieldPart part) {
      return val >= part.from && val <= part.to && (val - part.from) % part.increment == 0;
    }
  }

  static class SimpleField extends BasicField {
    SimpleField(CronFieldType fieldType, String fieldExpr) {
      super(fieldType, fieldExpr);
    }

    boolean matches(int val) {
      if (val >= fieldType.from && val <= fieldType.to) {
        for (FieldPart part : parts) {
          if (matches(val, part)) {
            return true;
          }
        }
      }
      return false;
    }
  }

  static class DayOfWeekField extends BasicField {

    DayOfWeekField(String fieldExpr) {
      super(CronFieldType.DAY_OF_WEEK, fieldExpr);
    }

    boolean matches(LocalDate date) {
      for (FieldPart part : parts) {
        if ("L".equals(part.modifier)) {
          YearMonth ym = YearMonth.of(date.getYear(), date.getMonth().getValue());
          return date.getDayOfWeek() == DayOfWeek.of(part.from)
              && date.getDayOfMonth() > (ym.lengthOfMonth() - 7);
        } else if ("#".equals(part.incrementModifier)) {
          if (date.getDayOfWeek() == DayOfWeek.of(part.from)) {
            int num = date.getDayOfMonth() / 7;
            return part.increment == (date.getDayOfMonth() % 7 == 0 ? num : num + 1);
          }
          return false;
        } else if (matches(date.getDayOfWeek().getValue(), part)) {
          return true;
        }
      }
      return false;
    }

    @Override
    protected boolean matches(int val, FieldPart part) {
      return "?".equals(part.modifier) || super.matches(val, part);
    }

    @Override
    protected Integer mapValue(String value) {
      // Use 1-7 for weekdays, but 0 will also represent sunday (linux practice)
      return "0".equals(value) ? Integer.valueOf(7) : super.mapValue(value);
    }

    @Override
    protected void validatePart(FieldPart part) {
      if (part.modifier != null && !Arrays.asList("L", "?").contains(part.modifier)) {
        throw new IllegalArgumentException(String.format("Invalid modifier [%s]", part.modifier));
      } else if (part.incrementModifier != null
          && !Arrays.asList("/", "#").contains(part.incrementModifier)) {
        throw new IllegalArgumentException(
            String.format("Invalid increment modifier [%s]", part.incrementModifier));
      }
    }
  }

  static class DayOfMonthField extends BasicField {
    DayOfMonthField(String fieldExpr) {
      super(CronFieldType.DAY_OF_MONTH, fieldExpr);
    }

    boolean matches(LocalDate date) {
      for (FieldPart part : parts) {
        if ("L".equals(part.modifier)) {
          YearMonth ym = YearMonth.of(date.getYear(), date.getMonth().getValue());
          return date.getDayOfMonth() == (ym.lengthOfMonth() - (part.from == null ? 0 : part.from));
        } else if ("W".equals(part.modifier)) {
          if (date.getDayOfWeek().getValue() <= 5) {
            if (date.getDayOfMonth() == part.from) {
              return true;
            } else if (date.getDayOfWeek().getValue() == 5) {
              return date.plusDays(1).getDayOfMonth() == part.from;
            } else if (date.getDayOfWeek().getValue() == 1) {
              return date.minusDays(1).getDayOfMonth() == part.from;
            }
          }
        } else if (matches(date.getDayOfMonth(), part)) {
          return true;
        }
      }
      return false;
    }

    @Override
    protected boolean matches(int val, FieldPart part) {
      return "?".equals(part.modifier) || super.matches(val, part);
    }

    @Override
    protected void validatePart(FieldPart part) {
      if (part.modifier != null && !Arrays.asList("L", "W", "?").contains(part.modifier)) {
        throw new IllegalArgumentException(String.format("Invalid modifier [%s]", part.modifier));
      } else if (part.incrementModifier != null && !"/".equals(part.incrementModifier)) {
        throw new IllegalArgumentException(
            String.format("Invalid increment modifier [%s]", part.incrementModifier));
      }
    }
  }
}
