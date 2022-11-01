package ru.mvz.elasticsearch.service;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class IndexEvent {

    private String action;

    private String id;

    private String indexName;

    private String indexType;

}