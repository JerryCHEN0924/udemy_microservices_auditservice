package tw.com.hanjiCHEN.auditservice.products.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

public record ProductEventApiPageDto(
        List<ProductEventApiDto> items,
        /*
        Jackson（Java 的 JSON 處理庫）提供的一個註解，用來控制對象在序列化成 JSON 時是否包含值為 null 的屬性。
        當你使用 @JsonInclude(JsonInclude.Include.NON_NULL) 註解在類或屬性上時，Jackson 會忽略所有值為 null 的屬性，
        不會將它們包含在最終生成的 JSON 中。
         */
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String lastEvaluatedTimestamp,
        int count
) {
}
