import com.rrc.bigdata.connector.MySqlSinkClickHouseConnector;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.time.temporal.ChronoField.MILLI_OF_SECOND;

public class ConnectorTest {

    @Test
    public void validateTest() {
        Map<String, String> connectorConfigs = new HashMap<>(16);
        MySqlSinkClickHouseConnector connector = new MySqlSinkClickHouseConnector();

        connector.validate(connectorConfigs);


    }

    @Test
    public void testDate() throws ParseException {
//        System.out.println(Long.parseLong("28373.0"));

        SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
        Date parse = format.parse("03/Dec/2018:14:36:31 +0800");
        System.out.println(parse);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
        LocalDateTime localDateTime = LocalDateTime.parse("03/Dec/2018:14:36:31 +0800", formatter);
        System.out.println(localDateTime);

        LocalDate localDate = LocalDate.parse("03/Dec/2018:14:36:31 +0800", formatter);
        System.out.println(localDate);

        DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("dd/MMM/yyyy Z", Locale.US);
        LocalDate localDateTime1 = LocalDate.parse("03/Dec/2018 +0800", formatterDate);
        System.out.println(localDateTime1);


        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Date parse1 = df.parse("2013-09-25T15:16:35+08:00");
        SimpleDateFormat dft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(dft.format(parse1));
    }

    @Test
    public void testLocalDateTime() throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = df.parse("2018-12-12 12:01:22");

        DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        String format = formatterDate.format(LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()));
        System.out.println(format);
    }

    @Test
    public void utcDateTime() {

        DateTimeFormatter utcFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        LocalDateTime dateTime = LocalDateTime.parse("2018-10-31T15:31:45Z", utcFormatter);
        dateTime = dateTime.plus(8L, ChronoUnit.HOURS);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println(formatter.format(dateTime));

        System.out.println(dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());

        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli()), ZoneId.systemDefault());
//        LocalDateTime time = LocalDateTime.ofInstant(new Date(dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli()).toInstant(), ZoneId.systemDefault());

        DateTimeFormatter YEAR_DF = DateTimeFormatter.ofPattern("yyyy");
        System.out.println(YEAR_DF.format(time));
        DateTimeFormatter DATE_DF = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        System.out.println(DATE_DF.format(time));
        DateTimeFormatter TIME_DF = DateTimeFormatter.ofPattern("HH:mm:ss");
        System.out.println(TIME_DF.format(time));
    }

    @Test
    public void utcDateTime800() {

        DateTimeFormatter utcFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss+08:00");
        LocalDateTime dateTime = LocalDateTime.parse("2018-12-24T03:50:06+08:00", utcFormatter);
        dateTime = dateTime.plus(8L, ChronoUnit.HOURS);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        System.out.println(formatter.format(dateTime));

        System.out.println(dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());

        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli()), ZoneId.systemDefault());
//        LocalDateTime time = LocalDateTime.ofInstant(new Date(dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli()).toInstant(), ZoneId.systemDefault());

        DateTimeFormatter YEAR_DF = DateTimeFormatter.ofPattern("yyyy");
        System.out.println(YEAR_DF.format(time));
        DateTimeFormatter DATE_DF = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        System.out.println(DATE_DF.format(time));
        DateTimeFormatter TIME_DF = DateTimeFormatter.ofPattern("HH:mm:ss");
        System.out.println(TIME_DF.format(time));
    }

    @Test
    public void testDebeziumDate() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse("1970-01-01 00:00:00", formatter);


        LocalDateTime plus = dateTime.plus(1L, ChronoUnit.DAYS);
        DateTimeFormatter DATE_DF = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        System.out.println(DATE_DF.format(plus));
    }

    @Test
    public void testDebeziumYear() throws ParseException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");
        Date parse = dateFormat.parse("2018");



        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(parse.getTime()), ZoneId.systemDefault());
        System.out.println(formatter.format(time));
    }


}
