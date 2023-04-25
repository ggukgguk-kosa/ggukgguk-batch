package com.ggukgguk.batch.common.config;

import com.ggukgguk.batch.common.service.ComprehendService;
import com.ggukgguk.batch.common.service.ComprehendServiceImpl;
import com.ggukgguk.batch.common.service.RekognizeService;
import com.ggukgguk.batch.common.service.RekognizeServiceImpl;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeansConfig {
    @Bean
    public ComprehendService comprehendService() {
        return new ComprehendServiceImpl();
    }

    @Bean
    public RekognizeService rekognizeService() {
        return new RekognizeServiceImpl();
    }
}
