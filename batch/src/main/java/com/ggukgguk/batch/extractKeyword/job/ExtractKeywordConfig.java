package com.ggukgguk.batch.extractKeyword.job;

import com.ggukgguk.batch.common.service.ComprehendService;
import com.ggukgguk.batch.common.service.ComprehendServiceImpl;
import com.ggukgguk.batch.extractKeyword.vo.Record;
import com.ggukgguk.batch.extractKeyword.vo.RecordKeyword;
import com.ggukgguk.batch.extractKeyword.vo.RecordKeywordExtended;
import com.ggukgguk.batch.extractKeyword.writer.ItemListWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class ExtractKeywordConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;
    private final ComprehendService comprehendService;

    private static final int chunkSize = 10;

    @Bean
    public Job extractKeywordJob() {
        return jobBuilderFactory.get("extractKeywordJob")
                .start(getKeywordsStep())
                .next(countKeywordsStep())
                .build();
    }

    @Bean
    public Step getKeywordsStep() {
        return stepBuilderFactory.get("getKeywordsStep")
                .<Record, List<RecordKeyword>> chunk(chunkSize)
                .reader(recordReader(null))
                .processor(recordProcessor())
                .writer(recordWriter())
                .build();
    }

    @Bean
    public Step countKeywordsStep() {
        return stepBuilderFactory.get("countKeywordsStep")
                .<RecordKeywordExtended, RecordKeywordExtended> chunk(chunkSize)
                .reader(recordKeywordReader(null))
                .writer(recordKeywordWriter())
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<Record> recordReader(
            @Value("#{jobParameters[executionDate]}") Date executionDate) {

        int year = executionDate.getYear();
        int month = executionDate.getMonth();
        Date startDate = new Date(year, month, 1);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        return new JdbcCursorItemReaderBuilder<Record>()
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Record.class))
                .sql("SELECT *\n" +
                        "FROM record\n" +
                        "WHERE record_created_at\n" +
                        "BETWEEN '" + format.format(startDate) + " 00:00'\n" +
                        "\tAND DATE_ADD('" + format.format(startDate) + " 00:00', INTERVAL +1 month);") // Job 파라미터에서 받아오도록 수정하기
                .name("recordReader")
                .build();
    }

    @Bean
    public ItemProcessor<Record, List<RecordKeyword>> recordProcessor() {
        return record -> {
            List<RecordKeyword> result = comprehendService.extractKeywordFromRecord(record);
            return result;
        };
    }

    @Bean
    public ItemWriter<List<RecordKeyword>> recordWriter() {
        return new ItemListWriter<RecordKeyword>(new JdbcBatchItemWriterBuilder<RecordKeyword>()
                .dataSource(dataSource)
                .sql("INSERT IGNORE INTO record_keyword VALUES (NULL, ?, ?)")
                .beanMapped()
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setInt(1, item.getRecordId());
                    ps.setString(2, item.getRecordKeyword());
                })
                .assertUpdates(false)
                .build());
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<RecordKeywordExtended> recordKeywordReader(
            @Value("#{jobParameters[executionDate]}") Date executionDate) {

        int year = executionDate.getYear();
        int month = executionDate.getMonth();
        Date startDate = new Date(year, month, 1);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        return new JdbcCursorItemReaderBuilder<RecordKeywordExtended>()
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(RecordKeywordExtended.class))
                .sql("SELECT YEAR(r.record_created_at) AS 'diary_year', MONTH(r.record_created_at) AS 'diary_month', r.member_id, rk.record_keyword FROM record_keyword rk\n" +
                        "LEFT JOIN record r \n" +
                        "ON r.record_id = rk.record_id\n" +
                        "WHERE r.record_created_at BETWEEN '" + format.format(startDate) +"' 00:00" +
                        "\tAND DATE_ADD('" + format.format(startDate) + " 00:00', INTERVAL +1 month);") // Job 파라미터에서 받아오도록 수정하기
                .name("recordKeywordReader")
                .build();
    }

    public JdbcBatchItemWriter<RecordKeywordExtended> insertDiaryEntity() {
        return new JdbcBatchItemWriterBuilder<RecordKeywordExtended>()
                .dataSource(dataSource)
                .sql("INSERT IGNORE INTO diary VALUES (NULL, ?, ?, ?, '', '')")
                .beanMapped()
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setString(1, item.getMemberId());
                    ps.setString(2, item.getDiaryYear());
                    ps.setString(3, item.getDiaryMonth());
                })
                .assertUpdates(false) // INSERT IGNORE이므로 affectedRows가 0일 수 있음
                .build();
    }

    public JdbcBatchItemWriter<RecordKeywordExtended> insertOrUpdateKeywordFreq() {
        return new JdbcBatchItemWriterBuilder<RecordKeywordExtended>()
                .dataSource(dataSource)
                .sql("INSERT INTO diary_keyword\n" +
                        "VALUES (" +
                        "    NULL," +
                        "    (SELECT diary_id FROM diary WHERE member_id = ? AND diary_year = ? AND diary_month = ?)," +
                        "    ?," +
                        "    1" +
                        ")" +
                        "ON DUPLICATE KEY\n" +
                        "UPDATE diary_freq = diary_freq + 1;")
                .beanMapped()
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setString(1, item.getMemberId());
                    ps.setString(2, item.getDiaryYear());
                    ps.setString(3, item.getDiaryMonth());
                    ps.setString(4, item.getRecordKeyword());
                })
                .build();
    }

    @Bean
    public CompositeItemWriter<RecordKeywordExtended> recordKeywordWriter() {
        return new CompositeItemWriterBuilder<RecordKeywordExtended>()
                .delegates(Arrays.asList(insertDiaryEntity(), insertOrUpdateKeywordFreq()))
                .build();
    }

}
