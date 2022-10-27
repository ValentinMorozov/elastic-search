package ru.mvz.elasticsearch.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.Document;

import java.util.*;

import static ru.mvz.elasticsearch.util.Package.splitByDot;

/**
 * Класс содержит поля, используемые для создания полей индекса Elastic Search
 * и генерации запросов к базе данных MongoDB.
 *
 * @author  Валентин Морозов
 * @since   1.0
 */
@Setter
@Getter
@NoArgsConstructor
public class MongoFields {
    /**
     * Набор полей, используемых для создания индекса. Имя поля преобразовано из формата
     * с разделителем "\\." в формат списка строк
     */
    private Set<List<String>> fields;
    /**
     * Имя интегрированного поля
     */
    private String summaryFieldName;
    /**
     * Разделитель, используемый при заполнении интегрированного поля
     */
    private String summaryFieldSeparator;
    /**
     * Набор полей, используемый при генерации интегрированного поля
     */
    private Set<List<String>> summaryFieldFields;
    /**
     * Проекция для запроса данных из MongoDB
     */
    private Document projection;
    /**
     * Устанавливает значение поля {@code fields}
     *
     * @param fields набор имен полей
     */
    public void setFields(Collection<List<String>> fields) {
        this.fields = new HashSet<>(splitByDot(fields.stream()));
    }
    /**
     * Формирует и устанавливает значение поля {@code projection} из списка имен полей
     *
     * @param fields список имен полей
     */
    public void setProjection(List<? extends List<String>> fields) {
        this.projection = new Document();
        for(List<String> list: fields) {
            projection.append(String.join(".", list), 1);
        }
    }
    /**
     * Устанавливает значение поля {@code SummaryFieldFields}
     * Перед зависью поля оно разбивается на составные части, разделенный точками
     *
     * @param fields набор имен полей
     */
    public void setSummaryFieldFields(Collection<List<String>> fields) {
        this.summaryFieldFields = new HashSet<>(splitByDot(fields.stream()));
    }

}

