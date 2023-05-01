package com.ggukgguk.batch.extractKeyword.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Keyword {
    private String word;
    private float score;
}
