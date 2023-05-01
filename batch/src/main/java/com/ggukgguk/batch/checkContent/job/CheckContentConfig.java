package com.ggukgguk.batch.checkContent.job;

import com.ggukgguk.batch.checkContent.vo.MediaFile;
import com.ggukgguk.batch.checkContent.service.RekognizeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import ws.schild.jave.EncoderException;
import ws.schild.jave.MultimediaObject;
import ws.schild.jave.ScreenExtractor;

import javax.sql.DataSource;
import java.io.File;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class CheckContentConfig {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;
    private final RekognizeService rekognizeService;

    private static final int chunkSize = 10;

    @Bean
    public Job checkModContentJob() {
        return jobBuilderFactory.get("checkModContentJob")
                .start(getUncheckedStep())
                .build();
    }

    @Bean
    public Step getUncheckedStep() {
        return stepBuilderFactory.get("getUncheckedStep")
                .<MediaFile, MediaFile> chunk(chunkSize)
                .reader(mediaFileReader())
                .processor(mediaFileProcessor(null))
                .writer(updateMediaFile())
                .faultTolerant()
                .skip(EncoderException.class)
                .skipLimit(1000)
                .build();
    }

    @Bean
    public JdbcCursorItemReader<MediaFile> mediaFileReader() {
        return new JdbcCursorItemReaderBuilder<MediaFile>()
                .fetchSize(chunkSize)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(MediaFile.class))
                .sql("SELECT * FROM media_file WHERE media_file_checked = FALSE;")
                .name("mediaFileReader")
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<MediaFile, MediaFile> mediaFileProcessor(@Value("#{jobParameters[filePath]}") String filePath) {
        return mediaFile -> {
            String mediaType;
            if (mediaFile.getMediaTypeId().equals("video")
                    || mediaFile.getMediaTypeId().equals("VIDEO")) {
                mediaType = "video";
            } else if (mediaFile.getMediaTypeId().equals("IMAGE")
                    || mediaFile.getMediaTypeId().equals("image")) {
                mediaType = "image";
            } else {
                return mediaFile;
            }

            String sourcePath = filePath + "/" + mediaType + "/" + mediaFile.getMediaFileId();

            String checkFilePath;
            if (mediaType.equals("video")) {
                File baseDir = new File(filePath);
                File sourceFile = new File(sourcePath);

                if (!baseDir.exists()) {
                    try{
                        baseDir.mkdir(); // BaseDir이 없으면 생성
                    }
                    catch(Exception e){
                        e.getStackTrace();
                    }
                }

                File targetDir = new File(baseDir.getCanonicalPath() + "/thumbnail/" + sourceFile.getName()); // C:\dev\ggukgguk\files\thumbnail\a6f1004c-f2ec-4740-a418-c4bee4414f5f\
                if (!targetDir.exists()) {
                    try{
                        targetDir.mkdir(); // targetDir이 없으면 생성
                    }
                    catch(Exception e){
                        e.getStackTrace();
                    }
                }

                MultimediaObject sourceVideo = new MultimediaObject(new File(sourcePath));
                int width = -1;
                int height = -1;
                int seconds = 3;
                String fileNamePrefix = "EXTRACT_";
                String extension = "jpg";
                int quality = 0;
                ScreenExtractor screenExtractor = new ScreenExtractor();
                screenExtractor.render(
                        sourceVideo, width, height, seconds, targetDir, fileNamePrefix, extension, quality);
                File imgFiles[] = targetDir.listFiles();

                checkFilePath = imgFiles[imgFiles.length / 2].getCanonicalPath();
                log.debug("Selected stil cut: " + checkFilePath);
            } else {
                checkFilePath = sourcePath;
            }

            boolean shouldBlocked = rekognizeService.detectModLabel(checkFilePath);

            mediaFile.setMediaFileBlocked(shouldBlocked);
            mediaFile.setMediaFileChecked(true);
            return mediaFile;
        };
    }

    public JdbcBatchItemWriter<MediaFile> updateMediaFile() {
        return new JdbcBatchItemWriterBuilder<MediaFile>()
                .dataSource(dataSource)
                .sql("UPDATE media_file\n" +
                        "\tSET media_file_blocked = ?, media_file_checked = ?\n" +
                        "\tWHERE media_file_id = ?;")
                .beanMapped()
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setBoolean(1, item.isMediaFileBlocked());
                    ps.setBoolean(2, item.isMediaFileChecked());
                    ps.setString(3, item.getMediaFileId());
                })
                .build();
    }
}
