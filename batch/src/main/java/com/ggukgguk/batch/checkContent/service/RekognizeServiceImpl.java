package com.ggukgguk.batch.checkContent.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectModerationLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.DetectModerationLabelsResponse;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.ModerationLabel;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

@Slf4j
@Component
public class RekognizeServiceImpl implements RekognizeService{
    @Override
    public boolean detectModLabel(String sourceImage) {
        boolean shouldBlocked = false;

        Region region = Region.US_WEST_2;
        RekognitionClient rekClient = RekognitionClient.builder()
                .region(region)
                .build();

        try {
            InputStream sourceStream = new FileInputStream(sourceImage);
            SdkBytes sourceBytes = SdkBytes.fromInputStream(sourceStream);
            Image souImage = Image.builder()
                    .bytes(sourceBytes)
                    .build();

            DetectModerationLabelsRequest moderationLabelsRequest = DetectModerationLabelsRequest.builder()
                    .image(souImage)
                    .minConfidence(60F)
                    .build();

            DetectModerationLabelsResponse moderationLabelsResponse = rekClient.detectModerationLabels(moderationLabelsRequest);
            List<ModerationLabel> labels = moderationLabelsResponse.moderationLabels();
            log.debug("Detected labels for image (Total: " + labels.size() + ")");

            for (ModerationLabel label : labels) {
                log.debug("Label: " + label.name()
                        + "\n Confidence: " + label.confidence().toString() + "%"
                        + "\n Parent:" + label.parentName());
            }

            shouldBlocked = labels.size() > 0 ? true : false;
            log.debug("이미지 블록 여부: " + shouldBlocked);
        } catch (Exception e) {
            e.printStackTrace();
            log.info("AWS Rekognize 요청에 실패했습니다.");
            shouldBlocked = false;
        } finally {
            rekClient.close();
            return shouldBlocked;
        }
    }
}
